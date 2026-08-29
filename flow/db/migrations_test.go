package db

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// TestGooseBootstrapFromRefinery proves the goose migration path produces the exact same
// catalog schema as the legacy refinery path.
//   - Reference schema: cluster with all migrations run by refinery
//   - Comparison schemas:
//     1. a brand-new cluster: no refinery ledger exists, goose runs everything
//     2. a cluster on a much older version: goose runs the remaining migrations
//     3. a cluster that's on latest version: goose is a noop
func TestGooseBootstrapFromRefinery(t *testing.T) {
	ctx := context.Background()
	cfg := internal.GetCatalogPostgresConfigFromEnv(ctx)
	require.NotEmpty(t, cfg.Host, "PEERDB_CATALOG_HOST not set")

	refineryMigrations := readRefineryMigrations(t)
	refineryMaxVersion := refineryMigrations[len(refineryMigrations)-1].version
	gooseVersions := readGooseVersions(t)
	gooseMaxVersion := gooseVersions[len(gooseVersions)-1]
	require.GreaterOrEqual(t, gooseMaxVersion, refineryMaxVersion,
		"embedded goose migrations must include every refinery migration")

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	admin, err := pgx.Connect(ctx, connStr(ctx, cfg.Database))
	require.NoError(t, err, "catalog not reachable")
	t.Cleanup(func() { admin.Close(context.Background()) })

	baseDB := "test_db_migration_" + suffix
	createTestDB(t, admin, baseDB)
	applyRefineryMigrations(t, ctx, connStr(ctx, baseDB), refineryMigrations, refineryMaxVersion)
	baseSchema := schemaFingerprint(t, ctx, connStr(ctx, baseDB))
	require.NotEmpty(t, baseSchema)

	scenarios := []struct {
		name           string
		refineryCutoff int
	}{
		{"brand_new_cluster", 0},
		{"migrate_from_halfway", refineryMaxVersion / 2},
		{"migrate_from_latest", refineryMaxVersion},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			compDB := "test_db_migration_" + scenario.name + "_" + suffix
			createTestDB(t, admin, compDB)
			conn, err := pgx.Connect(ctx, connStr(ctx, compDB))
			require.NoError(t, err)
			defer conn.Close(ctx)

			applyRefineryMigrations(t, ctx, connStr(ctx, compDB), refineryMigrations, scenario.refineryCutoff)
			var refineryRows int
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT count(*) FROM refinery_schema_history").Scan(&refineryRows))
			require.Equal(t, scenario.refineryCutoff, refineryRows)

			require.NoError(t, Apply(ctx, connStr(ctx, compDB)))
			require.Equal(t, baseSchema, schemaFingerprint(t, ctx, connStr(ctx, compDB)),
				"goose result diverged from refinery schema")

			var ledgerRows, maxVersionId int
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT count(*) FILTER (WHERE version_id > 0), max(version_id) FROM goose_db_version`,
			).Scan(&ledgerRows, &maxVersionId))
			require.Equal(t, gooseMaxVersion, ledgerRows, "goose ledger rows after Apply")
			require.Equal(t, gooseMaxVersion, maxVersionId, "highest recorded version after Apply")
		})
	}

	// test goose's behavior when the database is ahead of the binary (e.g. rollback)
	t.Run("database_ahead_of_binary", func(t *testing.T) {
		db := "test_db_rollabck_" + suffix
		createTestDB(t, admin, db)
		require.NoError(t, Apply(ctx, connStr(ctx, db)))

		conn, err := pgx.Connect(ctx, connStr(ctx, db))
		require.NoError(t, err)
		defer conn.Close(ctx)
		futureVersion := gooseMaxVersion + 1
		_, err = conn.Exec(ctx,
			"INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)",
			futureVersion)
		require.NoError(t, err)

		// An older binary does not contain futureVersion, but must still start
		// successfully after a release rollback.
		require.NoError(t, Apply(ctx, connStr(ctx, db)))
		var recorded bool
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM goose_db_version WHERE version_id = $1 AND is_applied)",
			futureVersion).Scan(&recorded))
		require.True(t, recorded, "future migration ledger entry was not preserved")
	})
}

// TestMigrationVersions enforces the migration numbering rules: each directory
// must be duplicate-free and gap-free starting at version 1, and while the
// refinery and goose directories coexist they must contain the same versions.
func TestMigrationVersions(t *testing.T) {
	sortedGooseVersion := readGooseVersions(t)
	for i, version := range sortedGooseVersion {
		require.Equal(t, i+1, version, "gap in flow/db/migrations: missing version %d", i+1)
	}

	sortedRefineryVersions := readRefineryMigrations(t)
	for i, migration := range sortedRefineryVersions {
		require.Equal(t, i+1, migration.version,
			"gap in nexus/catalog/migrations: missing version %d", i+1)
	}

	require.Len(t, sortedRefineryVersions, len(sortedGooseVersion),
		"nexus/catalog/migrations and flow/db/migrations must contain the same versions; "+
			"while both coexist, every migration is added to both directories")
}

type migrationFile struct {
	filename string
	name     string // descriptive part of the filename, if the pattern captures one
	version  int
}

func createTestDB(t *testing.T, admin *pgx.Conn, name string) {
	t.Helper()
	_, err := admin.Exec(context.Background(), "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
		require.NoError(t, err)
	})
}

func connStr(ctx context.Context, database string) string {
	cfg := internal.GetCatalogPostgresConfigFromEnv(ctx)
	cfg.Database = database
	return internal.GetPGConnectionString(cfg, "catalog_migrations_test")
}

type refineryMigration struct {
	name    string
	sql     string
	version int
}

// readMigrationFiles parses a migration directory listing: every entry must match fileRE
// and no two entries may claim the same version. Returns entries sorted by version.
func readMigrationFiles(t *testing.T, entries []fs.DirEntry, fileRE *regexp.Regexp, dirLabel string) []migrationFile {
	t.Helper()
	seen := make(map[int]string, len(entries))
	files := make([]migrationFile, 0, len(entries))
	for _, entry := range entries {
		m := fileRE.FindStringSubmatch(entry.Name())
		require.NotNil(t, m, "unexpected file in %s: %s", dirLabel, entry.Name())
		version, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		require.NotContains(t, seen, version,
			"%s and %s claim the same version in %s", entry.Name(), seen[version], dirLabel)
		seen[version] = entry.Name()
		name := entry.Name()
		if len(m) > 2 {
			name = m[2]
		}
		files = append(files, migrationFile{filename: entry.Name(), name: name, version: version})
	}
	require.NotEmpty(t, files, "no migration files found in %s", dirLabel)
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files
}

// readGooseVersions returns the sorted, unique versions of the embedded goose migration files.
func readGooseVersions(t *testing.T) []int {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	files := readMigrationFiles(t, entries, regexp.MustCompile(`^(\d+)_.*\.sql$`), "flow/db/migrations")
	versions := make([]int, len(files))
	for i, file := range files {
		require.Less(t, file.version, 100_000,
			"%s: version looks like a timestamp; create migrations with `goose create -s`", file.filename)
		versions[i] = file.version
	}
	return versions
}

// readRefineryMigrations loads the original migration files that shipped with Nexus.
func readRefineryMigrations(t *testing.T) []refineryMigration {
	t.Helper()
	dir := filepath.Join("..", "..", "nexus", "catalog", "migrations")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "nexus refinery migrations not found; if they were removed, delete this test")
	files := readMigrationFiles(t, entries, regexp.MustCompile(`^V(\d+)__(.+)\.sql$`), "nexus/catalog/migrations")
	migrations := make([]refineryMigration, len(files))
	for i, file := range files {
		sql, err := os.ReadFile(filepath.Join(dir, file.filename))
		require.NoError(t, err)
		migrations[i] = refineryMigration{version: file.version, name: file.name, sql: string(sql)}
	}
	return migrations
}

// applyRefineryMigrations replicates refinery's runner: each migration file is
// executed as a single multi-statement batch inside its own transaction and
// recorded in refinery_schema_history.
func applyRefineryMigrations(t *testing.T, ctx context.Context, connStr string, files []refineryMigration, upToVersion int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `CREATE TABLE refinery_schema_history(
		version INT4 PRIMARY KEY,
		name VARCHAR(255),
		applied_on VARCHAR(255),
		checksum VARCHAR(255))`)
	require.NoError(t, err)

	for _, migration := range files {
		if migration.version > upToVersion {
			break
		}
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, migration.sql)
		require.NoError(t, err, "refinery-style apply of V%d__%s failed", migration.version, migration.name)
		_, err = tx.Exec(ctx,
			"INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ($1, $2, $3, $4)",
			migration.version, migration.name, time.Now().UTC().Format(time.RFC3339), "0")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}
}

// schemaFingerprint returns a sorted snapshot of the database schema objects and table row counts.
//
// Example fingerprint:
//
//	schema: public
//	column: public.flows.id num=1 type=bigint notnull=true default=-
//	constraint: public.flows flows_pkey PRIMARY KEY (id)
//	index: public.flows CREATE UNIQUE INDEX flows_pkey ON public.flows USING btree (id)
//	sequence: public.flows_id_seq
//	rows: public.flows 3
func schemaFingerprint(t *testing.T, ctx context.Context, connStr string) []string {
	t.Helper()
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	const excludedTables = `('goose_db_version', 'refinery_schema_history', 'goose_db_version_id_seq')`
	queries := []string{
		// schema
		`SELECT 'schema: ' || nspname FROM pg_namespace
		 WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'`,
		// columns
		`SELECT format('column: %s.%s.%s num=%s type=%s notnull=%s default=%s',
			n.nspname, c.relname, a.attname, a.attnum,
			format_type(a.atttypid, a.atttypmod), a.attnotnull,
			coalesce(pg_get_expr(d.adbin, d.adrelid), '-'))
		 FROM pg_attribute a
		 JOIN pg_class c ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		 WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p', 'v', 'm')
		   AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
		   AND c.relname NOT IN ` + excludedTables,
		// constraints
		`SELECT format('constraint: %s.%s %s %s',
			n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid))
		 FROM pg_constraint con
		 JOIN pg_class c ON c.oid = con.conrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
		   AND c.relname NOT IN ` + excludedTables,
		// indexes
		`SELECT format('index: %s.%s %s', schemaname, tablename, indexdef) FROM pg_indexes
		 WHERE schemaname NOT LIKE 'pg\_%' AND schemaname <> 'information_schema'
		   AND tablename NOT IN ` + excludedTables,
		// sequences
		`SELECT format('sequence: %s.%s', schemaname, sequencename) FROM pg_sequences
		 WHERE schemaname NOT LIKE 'pg\_%' AND schemaname <> 'information_schema'
		   AND sequencename NOT IN ` + excludedTables,
	}

	var fingerprint []string
	for _, query := range queries {
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		fingerprint = append(fingerprint, lines...)
	}

	// row counts
	rows, err := conn.Query(ctx,
		`SELECT format('%I.%I', n.nspname, c.relname)
		 FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind IN ('r', 'p', 'v', 'm')
		   AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
		   AND c.relname NOT IN `+excludedTables)
	require.NoError(t, err)
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	for _, table := range tables {
		var count int64
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count))
		fingerprint = append(fingerprint, fmt.Sprintf("rows: %s %d", table, count))
	}

	sort.Strings(fingerprint)
	return fingerprint
}
