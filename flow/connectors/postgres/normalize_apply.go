package connpostgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type destinationTableIndexInfo struct {
	// a non-partial unique index on the destination uses only the (source) primary key columns,
	// so matching on the key finds at most one row
	isPrimaryKeyUnique bool
	// the destination has another immediate (non-deferrable) unique or exclusion index,
	// so a batch-final MERGE can collide on intermediate states (23505/23P01)
	hasSecondaryUniqueIndex bool
}

type batchTable struct {
	name string
	// the batch has a delete or an update with an old tuple for this table,
	// the only records primaryKeyMoves can match, so it is skipped otherwise
	changed bool
}

const (
	// keyed counts how many of an index's key columns are primary key columns ($2).
	// Column 1 (isPrimaryKeyUnique): some non-partial unique index uses only key columns.
	// Column 2 (hasSecondaryUniqueIndex): some immediate unique/exclusion index other than
	// the one on exactly the key columns.
	dstTableSQL = `SELECT coalesce(bool_or(indisunique AND indpred IS NULL AND keyed = indnkeyatts), false),
	coalesce(bool_or((indisunique OR indisexclusion) AND indimmediate
		AND NOT (indpred IS NULL AND keyed = indnkeyatts AND keyed = cardinality($2::text[]))), false)
	FROM (SELECT i.*, (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = i.indrelid
		AND a.attnum = ANY((i.indkey::int2[])[0:i.indnkeyatts-1]) AND a.attname = ANY($2::text[])) keyed
		FROM pg_index i WHERE i.indrelid = $1::regclass) i`
	batchTablesSQL = `SELECT _peerdb_destination_table_name,
	bool_or(_peerdb_record_type = 2 OR _peerdb_record_type = 1 AND _peerdb_match_data <> '{}')
	FROM %s.%s WHERE _peerdb_batch_id = $1 GROUP BY 1`
	primaryKeyMovesSQL = `SELECT EXISTS(SELECT FROM %s.%s WHERE _peerdb_batch_id = $1 AND _peerdb_destination_table_name = $2
	AND (_peerdb_record_type = 1 AND (%s) OR _peerdb_record_type = 2 AND (%s)))`
)

func (c *PostgresConnector) getDstTableIndexInfo(
	ctx context.Context, tx pgx.Tx, table string, keys []string,
) (destinationTableIndexInfo, error) {
	var d destinationTableIndexInfo
	qt, err := common.ParseTableIdentifier(table)
	if err != nil {
		return d, err
	}
	if err := tx.QueryRow(ctx, dstTableSQL, qt.String(), keys).Scan(&d.isPrimaryKeyUnique, &d.hasSecondaryUniqueIndex); err != nil {
		return d, fmt.Errorf("failed to read destination table %s: %w", table, err)
	}
	return d, nil
}

func (c *PostgresConnector) getBatchTables(
	ctx context.Context, tx pgx.Tx, batchID int64, gen *normalizeStmtGenerator,
) ([]batchTable, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(batchTablesSQL, c.metadataSchema, gen.rawTableName), batchID)
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (batchTable, error) {
		var t batchTable
		return t, row.Scan(&t.name, &t.changed)
	})
	return slices.DeleteFunc(tables, func(t batchTable) bool {
		if _, ok := gen.tableSchemaMapping[t.name]; !ok {
			c.logger.Warn("table not found in table to schema mapping", "table", t.name)
			return true
		}
		return false
	}), err
}

/*
primaryKeyMoves reports whether a MERGE on the primary key would apply table t's changes in this batch wrongly:
 1. An update whose old tuple has a different primary key than its new tuple, i.e. the row moved to a new key.
 2. A delete that carries no primary key value. This happens when the source's replica identity was changed to
    another index after the mirror was set up: pgoutput then sends only that index's columns, and a MERGE on the
    primary key would never match the row, losing the delete.

If either is found, normalizeTable applies the batch with the ordered replay instead.
*/
func (c *PostgresConnector) primaryKeyMoves(
	ctx context.Context, tx pgx.Tx, rawTableName string, batchID int64, t batchTable, primaryKeys []string,
) (bool, error) {
	moved := make([]string, 0, len(primaryKeys))
	null := make([]string, 0, len(primaryKeys))
	for _, primaryKey := range primaryKeys {
		primaryKey = utils.QuoteLiteral(primaryKey)
		// jsonb || lets the right side win, so this is the old key if the old tuple has one,
		// else the new key; plain _peerdb_match_data would be NULL ('{}') for every ordinary update
		moved = append(moved, fmt.Sprintf("(_peerdb_data || _peerdb_match_data)->%s IS DISTINCT FROM _peerdb_data->%s", primaryKey, primaryKey))
		null = append(null, fmt.Sprintf("_peerdb_data->>%s IS NULL", primaryKey))
	}
	var result bool
	err := tx.QueryRow(ctx, fmt.Sprintf(primaryKeyMovesSQL, c.metadataSchema, rawTableName,
		strings.Join(moved, " OR "), strings.Join(null, " OR ")), batchID, t.name).Scan(&result)
	return result, err
}

func (c *PostgresConnector) normalizeTable(
	ctx context.Context, tx pgx.Tx, batchID int64, t batchTable, gen *normalizeStmtGenerator,
) (int, error) {
	schema := gen.tableSchemaMapping[t.name]
	d, err := c.getDstTableIndexInfo(ctx, tx, t.name, schema.PrimaryKeyColumns)
	if err != nil {
		return 0, err
	}
	gen.dst[t.name] = d
	isPgTypeSystem := schema.System == protos.TypeSystem_PG
	// The MERGE keeps only the last change per key and matches on the new key. Replay every change in
	// source order instead when that is wrong up front: replica identity full without a unique key on the
	// destination, or user columns sharing the raw table's _peerdb_ prefix. Otherwise check for key moves.
	applyInSourceOrder := isPgTypeSystem && (schema.IsReplicaIdentityFull && !d.isPrimaryKeyUnique || slices.ContainsFunc(schema.Columns,
		func(col *protos.FieldDescription) bool { return strings.HasPrefix(col.Name, "_peerdb_") }))
	if isPgTypeSystem && !applyInSourceOrder && t.changed {
		if applyInSourceOrder, err = c.primaryKeyMoves(ctx, tx, gen.rawTableName, batchID, t, schema.PrimaryKeyColumns); err != nil {
			return 0, err
		}
	}
	if !applyInSourceOrder {
		// Another unique/exclusion index can reject the MERGE's batch-final rows even though the source
		// got there through valid intermediate states (e.g. swapping two values). Run the MERGE under a
		// savepoint and replay in source order if it hits 23505/23P01; the final state is the same.
		fallback := isPgTypeSystem && d.hasSecondaryUniqueIndex
		if fallback {
			if _, err := tx.Exec(ctx, "SAVEPOINT _peerdb_merge"); err != nil {
				return 0, err
			}
		}
		rows := 0
		for _, stmt := range gen.generateNormalizeStatements(t.name) {
			var ct pgconn.CommandTag
			if ct, err = tx.Exec(ctx, stmt, batchID, t.name); err != nil {
				break
			}
			rows += int(ct.RowsAffected())
		}
		if err == nil || !fallback || !shared.IsSQLStateError(err, pgerrcode.UniqueViolation, pgerrcode.ExclusionViolation) {
			return rows, err
		}
		c.logger.Info("normalize: batch-final MERGE conflicted, applying in source order", "table", t.name, "error", err)
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT _peerdb_merge"); err != nil {
			return 0, err
		}
	}
	_, err = tx.Exec(ctx, gen.generateOrderedStatement(t.name, batchID), pgx.QueryExecModeSimpleProtocol)
	return 0, err
}
