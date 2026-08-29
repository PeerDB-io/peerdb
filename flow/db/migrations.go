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
	defaultGooseTableName    = "goose_db_version"
)

func Run(ctx context.Context) error {
	return Apply(ctx, internal.GetCatalogConnectionStringFromEnv(ctx))
}

func Apply(ctx context.Context, connStr string) error {
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
		defaultGooseTableName, defaultRefineryTableName,
	).Scan(&gooseExists, &refineryExists); err != nil {
		return err
	}
	shouldBootstrap := !gooseExists && refineryExists
	if !shouldBootstrap {
		return tx.Commit()
	}

	slog.InfoContext(ctx, "Bootstrapping goose ledger from refinery")
	store, err := database.NewStore(database.DialectPostgres, defaultGooseTableName)
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
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (version_id, is_applied)
		 SELECT version, true
		 FROM %s ORDER BY version`,
			defaultGooseTableName, defaultRefineryTableName),
	); err != nil {
		return err
	}
	return tx.Commit()
}
