//go:build tilt

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

// refineryFixture is a catalog version for which testdata/refinery_v<n>_pgdump.sql holds the pg_dump
// of a catalog refinery migrated to that version
type refineryFixture int

const (
	noRefineryFixture      refineryFixture = 0
	halfwayRefineryFixture refineryFixture = 28
	lastRefineryFixture    refineryFixture = 57
)

// TestGooseBootstrapFromRefinery proves the goose migration path produces the exact same
// catalog schema as the legacy refinery path.
// Scenarios:
//  1. a brand-new cluster: no refinery ledger exists, goose runs everything
//  2. a cluster on a much older version: goose runs the remaining migrations
//  3. a cluster on refinery's last version: goose runs only the versions after it
func TestGooseBootstrapFromRefinery(t *testing.T) {
	ctx := context.Background()
	cfg := internal.GetCatalogPostgresConfigFromEnv(ctx)

	catalogContainer := os.Getenv("CI_CATALOG_CONTAINER")
	require.NotEmpty(t, catalogContainer, "missing CI_CATALOG_CONTAINER environment variable")

	gooseVersions := readGooseVersions(t)
	gooseMaxVersion := gooseVersions[len(gooseVersions)-1]
	require.GreaterOrEqual(t, gooseMaxVersion, int(lastRefineryFixture))

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	admin, err := pgx.Connect(ctx, connStr(ctx, cfg.Database))
	require.NoError(t, err, "catalog not reachable")
	t.Cleanup(func() { admin.Close(context.Background()) })

	scenarios := []struct {
		name           string
		refineryCutoff refineryFixture
	}{
		{"brand_new_cluster", noRefineryFixture},
		{"migrate_from_halfway", halfwayRefineryFixture},
		{"migrate_from_latest", lastRefineryFixture},
	}
	schemas := make(map[string]string, len(scenarios))
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			db := "test_db_migration_" + scenario.name + "_" + suffix
			createTestDB(t, admin, db)
			conn, err := pgx.Connect(ctx, connStr(ctx, db))
			require.NoError(t, err)
			defer conn.Close(ctx)

			// apply refinery migration up to cutoff
			applyRefineryMigrations(t, ctx, connStr(ctx, db), scenario.refineryCutoff)
			if scenario.refineryCutoff != noRefineryFixture {
				var refineryRows int
				require.NoError(t, conn.QueryRow(ctx,
					"SELECT count(*) FROM refinery_schema_history").Scan(&refineryRows))
				require.Equal(t, int(scenario.refineryCutoff), refineryRows)
			}

			// apply remaining migration with goose
			require.NoError(t, Apply(ctx, connStr(ctx, db)))

			// expect all rows to be applied
			var ledgerRows, maxVersionId int
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT count(*) FILTER (WHERE version_id > 0), max(version_id) FROM goose_db_version`,
			).Scan(&ledgerRows, &maxVersionId))
			require.Equal(t, gooseMaxVersion, ledgerRows, "unexpected ledger row count")
			require.Equal(t, gooseMaxVersion, maxVersionId, "unexpected max version")

			schemas[scenario.name] = pgSchemaDump(t, ctx, catalogContainer, db)
		})
	}

	reference := schemas[scenarios[0].name]
	for _, scenario := range scenarios[1:] {
		require.Equal(t, reference, schemas[scenario.name],
			"%s schema differs from %s", scenario.name, scenarios[0].name)
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

// TestMigrationVersions enforces the migration numbering rules: the directory
// must be duplicate-free and gap-free starting at version 1.
func TestMigrationVersions(t *testing.T) {
	gooseVersions := readGooseVersions(t)
	for i, version := range gooseVersions {
		require.Equal(t, i+1, version, "gap in flow/db/migrations: missing version %d", i+1)
	}
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

func applyRefineryMigrations(t *testing.T, ctx context.Context, connStr string, upToVersion refineryFixture) {
	t.Helper()
	if upToVersion == noRefineryFixture {
		return
	}
	name := "refinery_v" + strconv.Itoa(int(upToVersion)) + "_pgdump.sql"
	dump, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, string(dump))
	require.NoError(t, err)
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
