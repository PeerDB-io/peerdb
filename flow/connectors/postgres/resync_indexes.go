package connpostgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// resyncIndex is an index of the original destination table that is carried over to the
// _resync table when it is swapped in. The _resync table is created with only columns and
// a primary key, so without this every index added on the destination (manually or through
// PEERDB_PG_AUTOMATED_SCHEMA_DUMP) would be lost on resync.
type resyncIndex struct {
	// name is the index (or constraint) name on the original table, restored after the swap
	name string
	// tempName is what the index is built as on the _resync table until the swap
	tempName string
	// createSQL builds the index on the _resync table before the swap, empty for exclusion
	// constraints and for a primary key the _resync table already has
	createSQL string
	// constraintType is pg_constraint.contype ('p', 'u', 'x') when the index backs a constraint
	constraintType string
	// constraintDef is pg_get_constraintdef, used for exclusion constraints
	constraintDef string
	deferrable    bool
	deferred      bool
}

// Index definitions of a table, with pg_get_indexdef's "CREATE [UNIQUE] INDEX name ON [ONLY] table "
// prefix stripped so the rest can be applied to the _resync table. The prefix is rebuilt with
// quote_ident the same way pg_get_indexdef quotes it; if it does not match, index_body is NULL.
const getResyncIndexesSQL = `
SELECT i.indexrelid::int8, ic.relname, i.indisunique, coalesce(con.contype::text, ''),
	coalesce(pg_get_constraintdef(con.oid), ''), coalesce(con.condeferrable, false), coalesce(con.condeferred, false),
	CASE WHEN left(d.def, length(d.prefix)) = d.prefix THEN substr(d.def, length(d.prefix) + 1) END
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.conrelid = i.indrelid AND con.contype IN ('p', 'u', 'x')
CROSS JOIN LATERAL (SELECT pg_get_indexdef(i.indexrelid) AS def,
	'CREATE ' || CASE WHEN i.indisunique THEN 'UNIQUE ' ELSE '' END || 'INDEX ' || quote_ident(ic.relname) ||
	' ON ' || CASE WHEN ic.relkind = 'I' THEN 'ONLY ' ELSE '' END ||
	quote_ident(n.nspname) || '.' || quote_ident(t.relname) || ' ' AS prefix) d
WHERE n.nspname = $1 AND t.relname = $2 AND i.indisvalid AND i.indisready
ORDER BY ic.relname`

const getPrimaryKeyNameSQL = `
SELECT con.conname FROM pg_constraint con
JOIN pg_class t ON t.oid = con.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = $1 AND t.relname = $2 AND con.contype = 'p'`

// buildResyncIndexes reads the indexes of the original table and builds them on the _resync
// table under temporary names. Nothing writes to the _resync table between the snapshot and the
// swap, so a plain CREATE INDEX is used; IF NOT EXISTS keeps it idempotent across activity retries.
// An index that cannot be built (e.g. it references a column the _resync table does not have)
// is logged and skipped rather than failing the resync.
func (c *PostgresConnector) buildResyncIndexes(
	ctx context.Context,
	resyncTable *common.QualifiedTable,
	originalTable *common.QualifiedTable,
) ([]resyncIndex, error) {
	rows, err := c.conn.Query(ctx, getResyncIndexesSQL, originalTable.Namespace, originalTable.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes of %s: %w", originalTable, err)
	}
	type indexRow struct {
		body           *string
		name           string
		constraintType string
		constraintDef  string
		oid            int64
		unique         bool
		deferrable     bool
		deferred       bool
	}
	indexRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (indexRow, error) {
		var r indexRow
		err := row.Scan(&r.oid, &r.name, &r.unique, &r.constraintType, &r.constraintDef, &r.deferrable, &r.deferred, &r.body)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read indexes of %s: %w", originalTable, err)
	}
	if len(indexRows) == 0 {
		return nil, nil
	}

	var resyncHasPrimaryKey bool
	if err := c.conn.QueryRow(ctx, "SELECT EXISTS("+getPrimaryKeyNameSQL+")",
		resyncTable.Namespace, resyncTable.Table).Scan(&resyncHasPrimaryKey); err != nil {
		return nil, fmt.Errorf("failed to check primary key of %s: %w", resyncTable, err)
	}

	indexes := make([]resyncIndex, 0, len(indexRows))
	for _, r := range indexRows {
		idx := resyncIndex{
			name:           r.name,
			tempName:       fmt.Sprintf("_peerdb_resync_%d", r.oid),
			constraintType: r.constraintType,
			constraintDef:  r.constraintDef,
			deferrable:     r.deferrable,
			deferred:       r.deferred,
		}
		logAttrs := []any{slog.String("index", r.name), slog.String("table", originalTable.String())}

		// exclusion constraints cannot be attached with USING INDEX, they are added during the swap
		if r.constraintType == "x" || (r.constraintType == "p" && resyncHasPrimaryKey) {
			indexes = append(indexes, idx)
			continue
		}
		if r.body == nil {
			c.logger.Warn("[postgres] unable to parse index definition, index will not be preserved on resync", logAttrs...)
			continue
		}

		unique := ""
		if r.unique {
			unique = "UNIQUE "
		}
		idx.createSQL = fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s %s",
			unique, common.QuoteIdentifier(idx.tempName), resyncTable.String(), *r.body)
		c.logger.Info("[postgres] building index on resync table", append(logAttrs, slog.String("query", idx.createSQL))...)
		if _, err := c.conn.Exec(ctx, idx.createSQL); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			c.logger.Warn("[postgres] failed to build index on resync table, index will not be preserved on resync",
				append(logAttrs, slog.Any("error", err))...)
			continue
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// restoreResyncIndexes runs inside the swap transaction, after the original table was dropped and
// the _resync table renamed to its name: it gives the indexes their original names back and
// re-attaches the constraints they backed. Each index gets its own savepoint so one that cannot be
// restored is logged and skipped without aborting the swap.
func (c *PostgresConnector) restoreResyncIndexes(
	ctx context.Context,
	tx pgx.Tx,
	table *common.QualifiedTable,
	indexes []resyncIndex,
) error {
	for _, idx := range indexes {
		tempIndex := common.QualifiedTable{Namespace: table.Namespace, Table: idx.tempName}
		name := common.QuoteIdentifier(idx.name)

		var query string
		switch idx.constraintType {
		case "p":
			if idx.createSQL == "" {
				// the _resync table got its own primary key, keep the original name
				var currentName string
				if err := tx.QueryRow(ctx, getPrimaryKeyNameSQL, table.Namespace, table.Table).Scan(&currentName); err != nil {
					return fmt.Errorf("failed to get primary key of %s: %w", table, err)
				}
				if currentName == idx.name {
					continue
				}
				query = fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s",
					table.String(), common.QuoteIdentifier(currentName), name)
			} else {
				query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY USING INDEX %s",
					table.String(), name, common.QuoteIdentifier(idx.tempName))
			}
		case "u":
			query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE USING INDEX %s",
				table.String(), name, common.QuoteIdentifier(idx.tempName))
		case "x":
			query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", table.String(), name, idx.constraintDef)
		default:
			query = fmt.Sprintf("ALTER INDEX %s RENAME TO %s", tempIndex.String(), name)
		}
		if idx.constraintType == "p" || idx.constraintType == "u" {
			if idx.deferrable {
				query += " DEFERRABLE"
				if idx.deferred {
					query += " INITIALLY DEFERRED"
				}
			}
		}

		savepoint, err := tx.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to create savepoint for index %s: %w", idx.name, err)
		}
		if _, err := c.execWithLoggingTx(ctx, query, savepoint); err != nil {
			c.logger.Warn("[postgres] failed to restore index after resync",
				slog.String("index", idx.name), slog.String("table", table.String()), slog.Any("error", err))
			if err := savepoint.Rollback(ctx); err != nil {
				return fmt.Errorf("failed to roll back savepoint for index %s: %w", idx.name, err)
			}
			continue
		}
		if err := savepoint.Commit(ctx); err != nil {
			return fmt.Errorf("failed to release savepoint for index %s: %w", idx.name, err)
		}
	}
	return nil
}
