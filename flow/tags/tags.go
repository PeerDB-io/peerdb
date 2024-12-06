package tags

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func GetTags(ctx context.Context, catalogPool *pgxpool.Pool, flowName string) (map[string]string, error) {
	var tags map[string]string

	err := catalogPool.QueryRow(ctx, "SELECT tags FROM flows WHERE name = $1", flowName).Scan(&tags)
	if err != nil && err != pgx.ErrNoRows {
		slog.Error("error getting flow tags", slog.Any("error", err))
		return nil, err
	}

	if tags == nil {
		tags = make(map[string]string)
	}

	return tags, nil
}
