package connpostgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type dstTable struct {
	identity []string
}

const dstTableSQL = `SELECT coalesce(array_agg('"' || replace(attname, '"', '""') || '"'), '{}') FROM pg_attribute
	WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attidentity = 'a'`

func (c *PostgresConnector) getDstTable(ctx context.Context, tx pgx.Tx, table string) (dstTable, error) {
	var d dstTable
	qt, err := common.ParseTableIdentifier(table)
	if err != nil {
		return d, err
	}
	if err := tx.QueryRow(ctx, dstTableSQL, qt.String()).Scan(&d.identity); err != nil {
		return d, fmt.Errorf("failed to read destination table %s: %w", table, err)
	}
	return d, nil
}

func (c *PostgresConnector) normalizeTable(
	ctx context.Context, tx pgx.Tx, batchID int64, table string, gen *normalizeStmtGenerator,
) (int, error) {
	d, err := c.getDstTable(ctx, tx, table)
	if err != nil {
		return 0, err
	}
	gen.dst[table] = d
	rows := 0
	for _, stmt := range gen.generateNormalizeStatements(table) {
		ct, err := tx.Exec(ctx, stmt, batchID, table)
		if err != nil {
			return 0, err
		}
		rows += int(ct.RowsAffected())
	}
	return rows, nil
}
