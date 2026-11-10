package connpostgres

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *PostgresConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.conn.Query(ctx, "SELECT n.nspname || '.' || c.relname AS schema_table "+
		"FROM pg_class c "+
		"JOIN pg_namespace n ON c.relnamespace = n.oid "+
		"WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'"+
		" AND c.relkind IN ('r', 'v', 'm', 'f', 'p')")
	if err != nil {
		return nil, err
	}

	tables, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (c *PostgresConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	pgVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		return nil, err
	}
	// same table filter as GetTablesInSchema, so the counts match the tables pane
	relKindFilterExpr := "t.relkind IN ('r', 'p')"
	if pgVersion < shared.POSTGRES_13 {
		relKindFilterExpr = "t.relkind = 'r'"
	}
	rows, err := c.conn.Query(ctx, `SELECT n.nspname,
		count(DISTINCT t.relname) FILTER (WHERE `+relKindFilterExpr+` AND t.relispartition IS NOT TRUE)::int
		FROM pg_namespace n LEFT JOIN pg_class t ON t.relnamespace = n.oid
		WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
		GROUP BY n.nspname`)
	if err != nil {
		return nil, err
	}

	var schemas []string
	counts := make(map[string]int32)
	var schema string
	var count int32
	if _, err := pgx.ForEachRow(rows, []any{&schema, &count}, func() error {
		schemas = append(schemas, schema)
		counts[schema] = count
		return nil
	}); err != nil {
		return nil, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas, TableCounts: counts}, nil
}

func (c *PostgresConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	pgVersion, err := shared.GetMajorVersion(ctx, c.conn)
	if err != nil {
		c.logger.Error("unable to get pgversion for schema tables", slog.Any("error", err))
		return nil, err
	}

	relKindFilterExpr := "t.relkind IN ('r', 'p')"
	// publish_via_partition_root is only available in PG13 and above
	if pgVersion < shared.POSTGRES_13 {
		relKindFilterExpr = "t.relkind = 'r'"
	}

	rows, err := c.conn.Query(ctx, `SELECT DISTINCT ON (t.relname)
		t.relname,
		(con.contype = 'p' OR t.relreplident in ('i', 'f')) AS can_mirror,
		pg_size_pretty(pg_total_relation_size(t.oid))::text AS table_size,
		(t.relreplident = 'f') AS is_replica_identity_full,
		(t.relpersistence = 'u') AS is_unlogged,
		t.reltuples::bigint AS estimated_rows,
		COALESCE((SELECT array_agg(a.attname::text ORDER BY array_position(i.indkey::int2[], a.attnum))
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = t.oid AND
				CASE WHEN t.relreplident = 'i' THEN i.indisreplident ELSE i.indisprimary END
		), '{}') AS key_columns,
		CASE WHEN t.relkind = 'r' THEN pg_total_relation_size(t.oid) END AS total_bytes
	FROM pg_class t
	LEFT JOIN pg_namespace n ON t.relnamespace = n.oid
	LEFT JOIN pg_constraint con ON con.conrelid = t.oid
	WHERE n.nspname = $1 AND `+
		relKindFilterExpr+
		`AND t.relispartition IS NOT TRUE ORDER BY t.relname, can_mirror DESC;`, schema)
	if err != nil {
		c.logger.Info("failed to fetch tables", slog.Any("error", err))
		return nil, err
	}

	// tables small enough to count exactly on every listing
	var small []string
	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table pgtype.Text
		var hasPkeyOrReplica pgtype.Bool
		var tableSize pgtype.Text
		var isReplicaIdentityFull pgtype.Bool
		var isUnlogged bool
		var estimatedRows pgtype.Int8
		var keyColumns []string
		var totalBytes pgtype.Int8
		if err := rows.Scan(
			&table, &hasPkeyOrReplica, &tableSize, &isReplicaIdentityFull, &isUnlogged, &estimatedRows, &keyColumns, &totalBytes,
		); err != nil {
			return nil, err
		}
		var sizeOfTable string
		if tableSize.Valid {
			sizeOfTable = tableSize.String
		}
		hasPrimaryKeyOrReplicaIdentity := hasPkeyOrReplica.Valid && hasPkeyOrReplica.Bool
		canMirror := !cdcEnabled || (hasPrimaryKeyOrReplicaIdentity && !isUnlogged)

		// Exact count for small tables is filled in below; otherwise reltuples from the
		// last ANALYZE (-1 on PG14+ until the first one). n_live_tup was tried and dropped:
		// after a stats reset it was off by up to 99% on the production source.
		var rowCount *int64
		var rowCountKind string
		if estimatedRows.Valid && estimatedRows.Int64 >= 0 {
			rowCount, rowCountKind = &estimatedRows.Int64, "estimate"
		}
		if totalBytes.Valid && totalBytes.Int64 < smallTableCountBytes {
			small = append(small, table.String)
		}

		return &protos.TableResponse{
			TableName:                      table.String,
			CanMirror:                      canMirror,
			TableSize:                      sizeOfTable,
			IsReplicaIdentityFull:          isReplicaIdentityFull.Bool,
			IsUnlogged:                     isUnlogged,
			HasPrimaryKeyOrReplicaIdentity: hasPrimaryKeyOrReplicaIdentity,
			EstimatedRows:                  rowCount,
			RowCountKind:                   rowCountKind,
			PrimaryKeyColumns:              keyColumns,
		}, nil
	})
	if err != nil {
		slog.InfoContext(ctx, "failed to fetch publications", slog.Any("error", err))
		return nil, err
	}
	c.countSmallTables(ctx, schema, tables, small)
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

// below this total size count(*) takes milliseconds, so listings count these exactly
const smallTableCountBytes = 10 << 20

// countSmallTables replaces the row count of small tables with an exact one.
// Best effort: on failure they keep the estimated count.
func (c *PostgresConnector) countSmallTables(
	ctx context.Context, schema string, tables []*protos.TableResponse, small []string,
) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	counts, err := c.CountTableRows(ctx, schema, small)
	if err != nil {
		c.logger.Debug("exact row counts failed", slog.Any("error", err))
		return
	}
	for _, t := range tables {
		if n, ok := counts[t.TableName]; ok {
			t.EstimatedRows = &n
			t.RowCountKind = "exact"
		}
	}
}

// CountTableRows runs count(*) on each table in one round trip (UNION ALL).
// It reads every table fully, so cost grows with table size.
func (c *PostgresConnector) CountTableRows(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
	counts := make(map[string]int64, len(tables))
	if len(tables) == 0 {
		return counts, nil
	}
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		parts = append(parts, "SELECT "+utils.QuoteLiteral(table)+"::text, count(*) FROM "+
			common.QuoteIdentifier(schema)+"."+common.QuoteIdentifier(table))
	}
	rows, err := c.conn.Query(ctx, strings.Join(parts, " UNION ALL "))
	if err != nil {
		return nil, err
	}
	var name string
	var n int64
	if _, err := pgx.ForEachRow(rows, []any{&name, &n}, func() error {
		counts[name] = n
		return nil
	}); err != nil {
		return nil, err
	}
	return counts, nil
}

func (c *PostgresConnector) GetColumns(ctx context.Context, version uint32, schema string, table string) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.Query(ctx, `SELECT
    DISTINCT attname AS column_name,
    atttypid AS oid,
    format_type(atttypid, atttypmod) AS data_type,
    (pg_constraint.contype = 'p') AS is_primary_key,
    (idx.indkey IS NOT NULL) AS is_replica_identity,
    NOT pg_attribute.attnotnull AS nullable
	FROM pg_attribute
	JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
	JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
	LEFT JOIN pg_constraint ON pg_attribute.attrelid = pg_constraint.conrelid
		AND pg_attribute.attnum = ANY(pg_constraint.conkey)
		AND pg_constraint.contype = 'p'
	LEFT JOIN pg_index idx ON pg_attribute.attrelid = idx.indrelid
		AND ((idx.indisprimary AND pg_class.relreplident = 'd') OR idx.indisreplident)
		AND pg_attribute.attnum = ANY(idx.indkey)
		AND idx.indisvalid AND idx.indisready AND idx.indislive
	WHERE pg_namespace.nspname = $1
		AND relname = $2
		AND pg_attribute.attnum > 0
		AND NOT attisdropped
	ORDER BY column_name;`, schema, table)
	if err != nil {
		return nil, err
	}

	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.ColumnsItem, error) {
		var columnName pgtype.Text
		var oid uint32
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		var isReplicaIdentity pgtype.Bool
		var nullable pgtype.Bool
		if err := rows.Scan(&columnName, &oid, &datatype, &isPkey, &isReplicaIdentity, &nullable); err != nil {
			return nil, err
		}
		return &protos.ColumnsItem{
			Name:              columnName.String,
			Type:              datatype.String,
			IsKey:             isPkey.Bool,
			Qkind:             string(c.postgresOIDToQValueKind(oid, c.customTypeMapping, version)),
			IsReplicaIdentity: isReplicaIdentity.Bool,
			Nullable:          nullable.Bool,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}
