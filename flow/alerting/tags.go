package alerting

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

func GetTags(ctx context.Context, catalogPool shared.CatalogPool, flowName string) (map[string]string, error) {
	var tags map[string]string

	if err := catalogPool.QueryRow(
		ctx, "SELECT tags FROM flows WHERE name = $1", flowName,
	).Scan(&tags); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Error("error getting flow tags", slog.Any("error", err))
		return nil, err
	}

	if tags == nil {
		tags = make(map[string]string)
	}

	return tags, nil
}
