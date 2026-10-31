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
