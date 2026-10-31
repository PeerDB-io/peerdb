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

type dstTable struct {
	identity  []string
	keyUnique bool
	secondary bool
}

type batchTable struct {
	name    string
	changed bool
}

const (
	dstTableSQL = `SELECT (SELECT coalesce(array_agg('"' || replace(attname, '"', '""') || '"'), '{}') FROM pg_attribute
		WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attidentity = 'a'),
	coalesce(bool_or(indisunique AND indpred IS NULL AND keyed = indnkeyatts), false),
	coalesce(bool_or((indisunique OR indisexclusion) AND indimmediate
		AND NOT (indpred IS NULL AND keyed = indnkeyatts AND keyed = cardinality($2::text[]))), false)
	FROM (SELECT i.*, (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = i.indrelid
		AND a.attnum = ANY((i.indkey::int2[])[0:i.indnkeyatts-1]) AND a.attname = ANY($2::text[])) keyed
		FROM pg_index i WHERE i.indrelid = $1::regclass) i`
	batchTablesSQL = `SELECT _peerdb_destination_table_name,
	bool_or(_peerdb_record_type = 2 OR _peerdb_record_type = 1 AND _peerdb_match_data <> '{}')
	FROM %s.%s WHERE _peerdb_batch_id = $1 GROUP BY 1`
	keyMovesSQL = `SELECT EXISTS(SELECT FROM %s.%s WHERE _peerdb_batch_id = $1 AND _peerdb_destination_table_name = $2
	AND (_peerdb_record_type = 1 AND (%s) OR _peerdb_record_type = 2 AND (%s)))`
)

func (c *PostgresConnector) getDstTable(ctx context.Context, tx pgx.Tx, table string, keys []string) (dstTable, error) {
	var d dstTable
	qt, err := common.ParseTableIdentifier(table)
	if err != nil {
		return d, err
	}
	if err := tx.QueryRow(ctx, dstTableSQL, qt.String(), keys).Scan(&d.identity, &d.keyUnique, &d.secondary); err != nil {
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

func (c *PostgresConnector) keyMoves(ctx context.Context, tx pgx.Tx, raw string, batchID int64, t batchTable, keys []string) (bool, error) {
	moved := make([]string, 0, len(keys))
	null := make([]string, 0, len(keys))
	for _, k := range keys {
		k = utils.QuoteLiteral(k)
		moved = append(moved, fmt.Sprintf("(_peerdb_data || _peerdb_match_data)->%s IS DISTINCT FROM _peerdb_data->%s", k, k))
		null = append(null, fmt.Sprintf("_peerdb_data->>%s IS NULL", k))
	}
	var r bool
	err := tx.QueryRow(ctx, fmt.Sprintf(keyMovesSQL, c.metadataSchema, raw,
		strings.Join(moved, " OR "), strings.Join(null, " OR ")), batchID, t.name).Scan(&r)
	return r, err
}

func (c *PostgresConnector) normalizeTable(
	ctx context.Context, tx pgx.Tx, batchID int64, t batchTable, gen *normalizeStmtGenerator,
) (int, error) {
	schema := gen.tableSchemaMapping[t.name]
	d, err := c.getDstTable(ctx, tx, t.name, schema.PrimaryKeyColumns)
	if err != nil {
		return 0, err
	}
	gen.dst[t.name] = d
	pg := schema.System == protos.TypeSystem_PG
	ordered := pg && (schema.IsReplicaIdentityFull && !d.keyUnique || slices.ContainsFunc(schema.Columns,
		func(col *protos.FieldDescription) bool { return strings.HasPrefix(col.Name, "_peerdb_") }))
	if pg && !ordered && t.changed {
		if ordered, err = c.keyMoves(ctx, tx, gen.rawTableName, batchID, t, schema.PrimaryKeyColumns); err != nil {
			return 0, err
		}
	}
	if !ordered {
		fallback := pg && d.secondary
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
