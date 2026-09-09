package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Goose already uses a session-level advisory lock during migrations.
// This distinct lock protects the bootstrap-from-refinery process.
// crc32.ChecksumIEEE([]byte("peerdb"))
const bootstrapLockID int64 = 311574919

const (
	defaultRefineryTableName = "refinery_schema_history"
)

func Run(ctx context.Context) error {
	return Apply(ctx, internal.GetCatalogConnectionStringFromEnv(ctx))
}

func Apply(ctx context.Context, connStr string) error {
	slog.InfoContext(ctx, "Starting db migrations")

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to open catalog connection: %w", err)
	}
	defer db.Close()

	if err := bootstrapFromRefinery(ctx, db); err != nil {
		return fmt.Errorf("failed to bootstrap goose ledger from refinery_schema_history: %w", err)
	}

	sessionLocker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	migrationsRoot, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrationsRoot, goose.WithSessionLocker(sessionLocker))
	if err != nil {
		return err
	}

	results, err := provider.Up(ctx)
	for _, result := range results {
		slog.InfoContext(ctx, "Migration Applied",
			slog.String("name", result.Source.Path), slog.Int64("version", result.Source.Version))
	}
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	slog.InfoContext(ctx, "Completed db migrations")
	return nil
}

// bootstrapFromRefinery seeds the `goose_db_version` table from `refinery_schema_history`
// the first time the goose migrator runs against a catalog previously migrated by refinery.
// Versions map 1:1 between refinery's V<n>__<name>.sql files and goose's <n>_<name>.sql, so
// every version applied and recorded by refinery is marked applied in goose's ledger.
func bootstrapFromRefinery(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", bootstrapLockID); err != nil {
		return err
	}

	var gooseExists, refineryExists bool
	if err := tx.QueryRowContext(ctx,
		`SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL`,
		goose.DefaultTablename, defaultRefineryTableName,
	).Scan(&gooseExists, &refineryExists); err != nil {
		return err
	}
	shouldBootstrap := !gooseExists && refineryExists
	if !shouldBootstrap {
		return tx.Commit()
	}

	slog.InfoContext(ctx, "Bootstrapping goose ledger from refinery")
	store, err := database.NewStore(database.DialectPostgres, goose.DefaultTablename)
	if err != nil {
		return err
	}
	// Same DDL goose itself creates for Postgres
	if err := store.CreateVersionTable(ctx, tx); err != nil {
		return err
	}
	// Goose inserts version 0 when it creates its ledger.
	if err := store.Insert(ctx, tx, database.InsertRequest{Version: 0}); err != nil {
		return err
	}
	// Insert the refinery schema history into the goose ledger.
	if versions, err := getRefineryVersions(ctx, tx); err != nil {
		return err
	} else {
		for _, version := range versions {
			if err := store.Insert(ctx, tx, database.InsertRequest{Version: version}); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func getRefineryVersions(ctx context.Context, tx *sql.Tx) ([]int64, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT version FROM "+defaultRefineryTableName+" ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}
