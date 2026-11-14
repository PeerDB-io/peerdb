package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"
)

const ctidColumnName = "ctid"

// DefaultSnapshotPartitionKeys returns the partition key the initial load uses for each of the given tables
// ("schema.table") when none is configured. Tables without a default are read in full: everything before PG 14,
// which lacks TID range scans, and compressed hypertables.
func DefaultSnapshotPartitionKeys(
	ctx context.Context, conn *pgx.Conn, logger log.Logger, tables []string,
) (map[string]string, error) {
	keys := make(map[string]string, len(tables))

	var serverVersion int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&serverVersion); err != nil {
		return nil, fmt.Errorf("failed to determine server version: %w", err)
	}
	if serverVersion < 140000 {
		// older versions fall back to full table partitions anyway; nothing more to do
		logger.Warn("Postgres version does not support TID scans, falling back to full table partitions")
		return keys, nil
	}
	for _, table := range tables {
		keys[table] = ctidColumnName
	}

	var hasTimescale bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&hasTimescale); err != nil {
		return nil, fmt.Errorf("failed to check for timescaledb extension: %w", err)
	}
	if !hasTimescale {
		return keys, nil
	}

	// compressed hypertables cannot do ctid scans, so disable for them
	// NOTE: it appears that the hypercore "TAM" may give us access to ctid scans, but that's to be removed in Timescale 2.22
	rows, err := conn.Query(ctx, `SELECT DISTINCT hypertable_schema, hypertable_name
		FROM timescaledb_information.chunks
		WHERE is_compressed='t';`)
	if err != nil {
		return nil, fmt.Errorf("query compressed hypertables: %w", err)
	}
	var schema, name string
	if _, err := pgx.ForEachRow(rows, []any{&schema, &name}, func() error {
		table := fmt.Sprintf("%s.%s", schema, name)
		if _, ok := keys[table]; ok {
			delete(keys, table)
			logger.Warn("table is a compressed hypertable, falling back to full table partition", slog.String("table", table))
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to get compressed hypertables: %w", err)
	}
	return keys, nil
}

func UsesCTIDBlockPartitioning(watermarkColumn string, numPartitionsOverride uint32, ctidBlockPartitioning bool) bool {
	return watermarkColumn == ctidColumnName && numPartitionsOverride != 1 &&
		(ctidBlockPartitioning || numPartitionsOverride > 0)
}
