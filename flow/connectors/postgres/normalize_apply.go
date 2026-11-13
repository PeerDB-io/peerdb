package connpostgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresConnector) normalizeTable(
	ctx context.Context, tx pgx.Tx, batchID int64, table string, gen *normalizeStmtGenerator,
) (int, error) {
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
