package db

import (
	"context"
	"hash/crc32"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3/lock"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func TestBootstrapLockIDIsPinned(t *testing.T) {
	require.Equal(t, bootstrapLockID, int64(crc32.ChecksumIEEE([]byte("peerdb"))))
	require.NotEqual(t, lock.DefaultLockID, bootstrapLockID)
}

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

	nexusContainer := os.Getenv("CI_NEXUS_CONTAINER")
	require.NotEmpty(t, nexusContainer, "missing CI_NEXUS_CONTAINER environment variable")
	catalogContainer := os.Getenv("CI_CATALOG_CONTAINER")
	require.NotEmpty(t, catalogContainer, "missing CI_CATALOG_CONTAINER environment variable")

	refineryVersions := readRefineryVersions(t)
	refineryMaxVersion := refineryVersions[len(refineryVersions)-1]
	gooseVersions := readGooseVersions(t)
	gooseMaxVersion := gooseVersions[len(gooseVersions)-1]
	require.Equal(t, gooseMaxVersion, refineryMaxVersion)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	admin, err := pgx.Connect(ctx, connStr(ctx, cfg.Database))
	require.NoError(t, err, "catalog not reachable")
	t.Cleanup(func() { admin.Close(context.Background()) })

	baseDB := "test_db_migration_" + suffix
	createTestDB(t, admin, baseDB)
	applyRefineryMigrations(t, ctx, nexusContainer, baseDB, refineryMaxVersion)
	baseSchema := pgSchemaDump(t, ctx, catalogContainer, baseDB)
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

			// apply refinery migration up to cutoff
			applyRefineryMigrations(t, ctx, nexusContainer, compDB, scenario.refineryCutoff)
			var refineryRows int
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT count(*) FROM refinery_schema_history").Scan(&refineryRows))
			require.Equal(t, scenario.refineryCutoff, refineryRows)

			// apply remaining migration with goose
			require.NoError(t, Apply(ctx, connStr(ctx, compDB)))

			// expect all rows to be applied
			var ledgerRows, maxVersionId int
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT count(*) FILTER (WHERE version_id > 0), max(version_id) FROM goose_db_version`,
			).Scan(&ledgerRows, &maxVersionId))
			require.Equal(t, gooseMaxVersion, ledgerRows, "unexpected ledge row count")
			require.Equal(t, gooseMaxVersion, maxVersionId, "unexpected max version")

			// expect same pg schema
			require.Equal(t, baseSchema, pgSchemaDump(t, ctx, catalogContainer, compDB))
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
	gooseVersions := readGooseVersions(t)
	for i, version := range gooseVersions {
		require.Equal(t, i+1, version, "gap in flow/db/migrations: missing version %d", i+1)
	}

	refineryVersions := readRefineryVersions(t)
	for i, version := range refineryVersions {
		require.Equal(t, i+1, version,
			"gap in nexus/catalog/migrations: missing version %d", i+1)
	}

	require.Len(t, refineryVersions, len(gooseVersions),
		"nexus/catalog/migrations and flow/db/migrations must contain the same versions; "+
			"while both coexist, every migration is added to both directories")
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

// readMigrationVersions parses a migration directory listing: every entry must match the provided
// regex and no two entries may claim the same version. Returns the versions sorted ascending.
func readMigrationVersions(t *testing.T, entries []fs.DirEntry, regex *regexp.Regexp, migrationDir string) []int {
	t.Helper()
	seen := make(map[int]string, len(entries))
	versions := make([]int, 0, len(entries))
	for _, entry := range entries {
		m := regex.FindStringSubmatch(entry.Name())
		require.NotNil(t, m, "unexpected file in %s: %s", migrationDir, entry.Name())
		version, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		require.NotContains(t, seen, version,
			"%s and %s claim the same version in %s", entry.Name(), seen[version], migrationDir)
		seen[version] = entry.Name()
		versions = append(versions, version)
	}
	require.NotEmpty(t, versions, "no migration files found in %s", migrationDir)
	sort.Ints(versions)
	return versions
}

// readGooseVersions returns the sorted, unique versions of the embedded goose migration files.
func readGooseVersions(t *testing.T) []int {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	return readMigrationVersions(t, entries, regexp.MustCompile(`^(\d+)_.*\.sql$`), "flow/db/migrations")
}

// readRefineryVersions returns the sorted, unique versions of the refinery migration files.
func readRefineryVersions(t *testing.T) []int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("..", "..", "nexus", "catalog", "migrations"))
	require.NoError(t, err)
	return readMigrationVersions(t, entries, regexp.MustCompile(`^V(\d+)__(.+)\.sql$`), "nexus/catalog/migrations")
}

func applyRefineryMigrations(t *testing.T, ctx context.Context, container string, database string, upToVersion int) {
	t.Helper()
	// #nosec G702: test-controlled inputs
	cmd := exec.CommandContext(ctx,
		"docker", "exec", container,
		"./peerdb-server",
		"--migrations-only",
		"--catalog-database", database,
		"--migrations-target", strconv.Itoa(upToVersion))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "running nexus migrations:\n%s", output)
}

func pgSchemaDump(t *testing.T, ctx context.Context, container string, database string) string {
	t.Helper()
	// #nosec G702: test-controlled inputs
	cmd := exec.CommandContext(ctx,
		"docker", "exec", container,
		"pg_dump",
		"--no-owner",
		"--schema-only",
		"--exclude-table=public.refinery_schema_history",
		"--exclude-table=public.goose_db_version",
		"--exclude-table=public.goose_db_version_id_seq",
		"--dbname", database)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "pg_dump failed:\n%s", output)

	// pg_dump 18+ wraps the dump in \restrict/\unrestrict guards with a randomized token per invocation;
	// strip them so dumps are comparable.
	lines := slices.DeleteFunc(strings.Split(string(output), "\n"), func(line string) bool {
		return strings.HasPrefix(line, `\restrict `) || strings.HasPrefix(line, `\unrestrict `)
	})
	return strings.Join(lines, "\n")
}
