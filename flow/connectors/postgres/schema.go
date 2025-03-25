package connpostgres

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
	rows, err := c.conn.Query(ctx, "SELECT nspname"+
		" FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema';")
	if err != nil {
		return nil, err
	}

	schemas, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
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
		pg_size_pretty(pg_total_relation_size(t.oid))::text AS table_size
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

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.TableResponse, error) {
		var table pgtype.Text
		var hasPkeyOrReplica pgtype.Bool
		var tableSize pgtype.Text
		if err := rows.Scan(&table, &hasPkeyOrReplica, &tableSize); err != nil {
			return nil, err
		}
		var sizeOfTable string
		if tableSize.Valid {
			sizeOfTable = tableSize.String
		}
		canMirror := !cdcEnabled || (hasPkeyOrReplica.Valid && hasPkeyOrReplica.Bool)

		return &protos.TableResponse{
			TableName: table.String,
			CanMirror: canMirror,
			TableSize: sizeOfTable,
		}, nil
	})
	if err != nil {
		slog.Info("failed to fetch publications", slog.Any("error", err))
		return nil, err
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

func (c *PostgresConnector) GetColumns(ctx context.Context, schema string, table string) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.Query(ctx, `SELECT
    DISTINCT attname AS column_name,
    format_type(atttypid, atttypmod) AS data_type,
    (pg_constraint.contype = 'p') AS is_primary_key
	FROM pg_attribute
	JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
	JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
	LEFT JOIN pg_constraint ON pg_attribute.attrelid = pg_constraint.conrelid
		AND pg_attribute.attnum = ANY(pg_constraint.conkey)
		AND pg_constraint.contype = 'p'
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
		var datatype pgtype.Text
		var isPkey pgtype.Bool
		if err := rows.Scan(&columnName, &datatype, &isPkey); err != nil {
			return nil, err
		}
		return &protos.ColumnsItem{
			Name:  columnName.String,
			Type:  datatype.String,
			IsKey: isPkey.Bool,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}
