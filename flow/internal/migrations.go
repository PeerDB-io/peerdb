package internal

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// CheckMigrationCompleted checks if a migration has been completed for a given flow
func CheckMigrationCompleted(
	ctx context.Context,
	pool shared.CatalogPool,
	flowName string,
	migrationName string,
) (bool, error) {
	var completed bool
	err := pool.Pool.QueryRow(
		ctx,
		"SELECT completed FROM flow_migrations WHERE flow_name = $1 AND migration_name = $2",
		flowName,
		migrationName,
	).Scan(&completed)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Migration record doesn't exist, so it hasn't been completed
			return false, nil
		}
		return false, fmt.Errorf("failed to check migration status: %w", err)
	}

	return completed, nil
}

// MarkMigrationCompleted marks a migration as completed for a given flow
func MarkMigrationCompleted(
	ctx context.Context,
	pool shared.CatalogPool,
	logger log.Logger,
	flowName string,
	migrationName string,
) error {
	logger.Info("marking migration as completed",
		slog.String("flowName", flowName),
		slog.String("migrationName", migrationName))

	_, err := pool.Pool.Exec(
		ctx,
		`INSERT INTO flow_migrations (flow_name, migration_name, completed) 
         VALUES ($1, $2, true)
         ON CONFLICT (flow_name, migration_name) 
         DO UPDATE SET completed = true`,
		flowName,
		migrationName,
	)
	if err != nil {
		logger.Error("failed to mark migration as completed",
			slog.Any("error", err),
			slog.String("flowName", flowName),
			slog.String("migrationName", migrationName))
		return fmt.Errorf("failed to mark migration as completed: %w", err)
	}

	logger.Info("successfully marked migration as completed",
		slog.String("flowName", flowName),
		slog.String("migrationName", migrationName))

	return nil
}

// RunMigrationOnce runs a migration function only if it hasn't been completed for the given flow
func RunMigrationOnce(
	ctx context.Context,
	pool shared.CatalogPool,
	logger log.Logger,
	flowName string,
	migrationName string,
	migrationFunc func(ctx context.Context) error,
) error {
	// Check if migration has already been completed
	completed, err := CheckMigrationCompleted(ctx, pool, flowName, migrationName)
	if err != nil {
		return fmt.Errorf("failed to check migration status: %w", err)
	}

	if completed {
		logger.Info("migration already completed, skipping",
			slog.String("flowName", flowName),
			slog.String("migrationName", migrationName))
		return nil
	}

	logger.Info("running migration",
		slog.String("flowName", flowName),
		slog.String("migrationName", migrationName))

	if err := migrationFunc(ctx); err != nil {
		logger.Error("migration failed",
			slog.Any("error", err),
			slog.String("flowName", flowName),
			slog.String("migrationName", migrationName))
		return fmt.Errorf("migration failed: %w", err)
	}

	if err := MarkMigrationCompleted(ctx, pool, logger, flowName, migrationName); err != nil {
		return fmt.Errorf("failed to mark migration as completed: %w", err)
	}

	return nil
}
