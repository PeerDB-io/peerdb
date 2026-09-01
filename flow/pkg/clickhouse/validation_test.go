package clickhouse

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func init() {
	testutil.LoadEnv()
}

func TestProjectedPeakTableCount(t *testing.T) {
	tests := []struct {
		name                 string
		currentTables        uint64
		tableNames           []string
		existingTables       map[string]struct{}
		additionalTableCount uint64
		isResync             bool
		want                 uint64
	}{
		{
			name:                 "creation counts raw and missing destinations",
			currentTables:        10,
			tableNames:           []string{"existing", "missing"},
			existingTables:       map[string]struct{}{"existing": {}},
			additionalTableCount: 1,
			want:                 12,
		},
		{
			name:           "creation deduplicates destinations",
			currentTables:  10,
			tableNames:     []string{"missing", "missing"},
			existingTables: map[string]struct{}{},
			want:           11,
		},
		{
			name:           "resync counts absent tables persistently",
			currentTables:  10,
			tableNames:     []string{"first_resync", "second_resync"},
			existingTables: map[string]struct{}{},
			isResync:       true,
			want:           12,
		},
		{
			name:           "resync existing null table needs transient slot",
			currentTables:  10,
			tableNames:     []string{"null_table"},
			existingTables: map[string]struct{}{"null_table": {}},
			isResync:       true,
			want:           11,
		},
		{
			name:           "resync reserves transient slot after persistent tables",
			currentTables:  10,
			tableNames:     []string{"new_resync", "null_table"},
			existingTables: map[string]struct{}{"null_table": {}},
			isResync:       true,
			want:           12,
		},
		{
			name:           "resync calculation is conservative regardless of mapping order",
			currentTables:  10,
			tableNames:     []string{"null_table", "new_resync"},
			existingTables: map[string]struct{}{"null_table": {}},
			isResync:       true,
			want:           12,
		},
		{
			name:           "resync duplicate becomes replacement",
			currentTables:  10,
			tableNames:     []string{"new_resync", "new_resync"},
			existingTables: map[string]struct{}{},
			isResync:       true,
			want:           12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, projectedPeakTableCount(
				tt.currentTables,
				tt.tableNames,
				maps.Clone(tt.existingTables),
				tt.additionalTableCount,
				tt.isResync,
			))
		})
	}
}

func TestValidateTableCapacity(t *testing.T) {
	ctx := t.Context()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	adminConn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, adminConn.Close())
	})
	require.NoError(t, adminConn.Ping(ctx))

	database := "pkgch_capacity_" + strings.ToLower(common.RandomString(8))
	require.NoError(t, adminConn.Exec(ctx, "CREATE DATABASE "+QuoteIdentifier(database)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, adminConn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(database)))
	})

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	require.NoError(t, conn.Ping(ctx))

	const maxTables uint64 = 5000
	tablePrefix := "table_capacity_" + strings.ToLower(common.RandomString(8))
	tableNames := make([]string, int(maxTables)+1)
	for i := range tableNames {
		tableNames[i] = fmt.Sprintf("%s_%d", tablePrefix, i)
	}

	t.Run("one missing table fits", func(t *testing.T) {
		require.NoError(t, ValidateTableCapacity(t.Context(), nopLogger{}, conn, tableNames[:1], 0, false))
	})

	t.Run("missing tables and raw table exceed limit", func(t *testing.T) {
		err := ValidateTableCapacity(t.Context(), nopLogger{}, conn, tableNames[:maxTables], 1, false)
		var capacityErr *TableCapacityExceededError
		require.ErrorAs(t, err, &capacityErr)
		require.Equal(t, maxTables, capacityErr.MaxTables)
		require.Equal(t, maxTables+1, capacityErr.RequiredAdditionalTables)
	})

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id UInt64) ENGINE = MergeTree ORDER BY id",
		QuoteIdentifier(tableNames[0]),
	)))

	t.Run("existing table is skipped", func(t *testing.T) {
		err := ValidateTableCapacity(t.Context(), nopLogger{}, conn, tableNames, 0, false)
		var capacityErr *TableCapacityExceededError
		require.ErrorAs(t, err, &capacityErr)
		require.Equal(t, maxTables, capacityErr.MaxTables)
		require.Equal(t, maxTables, capacityErr.RequiredAdditionalTables)
	})

	t.Run("existing resync table reserves transient slot", func(t *testing.T) {
		err := ValidateTableCapacity(t.Context(), nopLogger{}, conn, tableNames, 0, true)
		var capacityErr *TableCapacityExceededError
		require.ErrorAs(t, err, &capacityErr)
		require.Equal(t, maxTables, capacityErr.MaxTables)
		require.Equal(t, maxTables+1, capacityErr.RequiredAdditionalTables)
	})
}

func TestCheckIfTablesEmptyAndEngine(t *testing.T) {
	ctx := t.Context()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	adminConn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, adminConn.Close())
	})
	require.NoError(t, adminConn.Ping(ctx))

	database := "pkgch_" + strings.ToLower(common.RandomString(8))
	require.NoError(t, adminConn.Exec(ctx, "CREATE DATABASE "+QuoteIdentifier(database)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, adminConn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(database)))
	})

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	require.NoError(t, conn.Ping(ctx))

	const (
		emptyTable       = "empty_table"
		nonEmptyTable    = "nonempty_table"
		mvSourceTable    = "materialized_view_source"
		mvTargetTable    = "materialized_view_target"
		view             = "test_view"
		materializedView = "test_materialized_view"
	)

	for _, statement := range []string{
		fmt.Sprintf("CREATE TABLE %s (id UInt64) ENGINE = ReplacingMergeTree ORDER BY id",
			QuoteIdentifier(emptyTable)),
		fmt.Sprintf("CREATE TABLE %s (id UInt64) ENGINE = ReplacingMergeTree ORDER BY id",
			QuoteIdentifier(nonEmptyTable)),
		fmt.Sprintf("CREATE TABLE %s (id UInt64) ENGINE = MergeTree ORDER BY id",
			QuoteIdentifier(mvSourceTable)),
		fmt.Sprintf("CREATE TABLE %s (id UInt64) ENGINE = MergeTree ORDER BY id",
			QuoteIdentifier(mvTargetTable)),
		fmt.Sprintf("CREATE VIEW %s AS SELECT id FROM %s",
			QuoteIdentifier(view), QuoteIdentifier(emptyTable)),
		fmt.Sprintf("CREATE MATERIALIZED VIEW %s TO %s AS SELECT id FROM %s",
			QuoteIdentifier(materializedView), QuoteIdentifier(mvTargetTable), QuoteIdentifier(mvSourceTable)),
		fmt.Sprintf("INSERT INTO %s VALUES (1)", QuoteIdentifier(nonEmptyTable)),
	} {
		require.NoError(t, conn.Exec(ctx, statement))
	}

	tablesAcrossChunks := make([]string, 200, 201)
	for i := range tablesAcrossChunks {
		tablesAcrossChunks[i] = fmt.Sprintf("missing_table_%d", i)
	}
	tablesAcrossChunks = append(tablesAcrossChunks, nonEmptyTable)

	tests := []struct {
		name                   string
		wantErr                string
		tables                 []string
		initialSnapshotEnabled bool
		checkForCloudSMT       bool
		allowNonEmpty          bool
	}{
		{
			name:    "view rejected",
			tables:  []string{view},
			wantErr: "destination table can not be a view",
		},
		{
			name:    "materialized view rejected",
			tables:  []string{materializedView},
			wantErr: "destination table can not be a view",
		},
		{
			name:                   "non-empty table with snapshot rejected",
			tables:                 []string{nonEmptyTable},
			initialSnapshotEnabled: true,
			wantErr:                fmt.Sprintf("table %s exists and is not empty", nonEmptyTable),
		},
		{
			name:                   "non-empty table allowed when allowNonEmpty",
			tables:                 []string{nonEmptyTable},
			initialSnapshotEnabled: true,
			allowNonEmpty:          true,
		},
		{
			name:   "non-empty table allowed without snapshot",
			tables: []string{nonEmptyTable},
		},
		{
			name:                   "empty acceptable table passes",
			tables:                 []string{emptyTable},
			initialSnapshotEnabled: true,
		},
		{
			name:             "non-shared engine fails cloud SMT check",
			tables:           []string{emptyTable},
			checkForCloudSMT: true,
			wantErr:          fmt.Sprintf("table %s exists and does not use SharedMergeTree engine", emptyTable),
		},
		{
			name:                   "tables are checked across query chunks",
			tables:                 tablesAcrossChunks,
			initialSnapshotEnabled: true,
			wantErr:                fmt.Sprintf("table %s exists and is not empty", nonEmptyTable),
		},
		{
			name:   "empty table list passes",
			tables: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckIfTablesEmptyAndEngine(
				t.Context(), nopLogger{}, conn,
				tt.tables, tt.initialSnapshotEnabled, tt.checkForCloudSMT, tt.allowNonEmpty,
			)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckIfClickHouseCloudHasSharedMergeTreeEnabled(t *testing.T) {
	ctx := t.Context()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	adminConn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, adminConn.Close())
	})
	require.NoError(t, adminConn.Ping(ctx))

	tests := []struct {
		name            string
		profileSettings string
		wantErr         string
	}{
		{
			name:            "cloud_mode_engine 2 accepted",
			profileSettings: "cloud_mode_engine = 2 READONLY",
		},
		{
			name:            "cloud_mode_engine 3 accepted",
			profileSettings: "cloud_mode_engine = 3 READONLY",
		},
		{
			name:            "cloud_mode_engine 4 accepted",
			profileSettings: "cloud_mode_engine = 4 READONLY",
		},
		{
			name:            "cloud_mode_engine 1 rejected",
			profileSettings: "cloud_mode_engine = 1 READONLY",
			wantErr:         "not migrated to use SharedMergeTree",
		},
		{
			name:            "non-readonly cloud_mode_engine rejected",
			profileSettings: "cloud_mode_engine = 2",
			wantErr:         "not migrated to use SharedMergeTree",
		},
		{
			name:    "default cloud_mode_engine rejected",
			wantErr: "not migrated to use SharedMergeTree",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			username := "smt_user_" + strings.ToLower(common.RandomString(8))
			require.NoError(t, adminConn.Exec(ctx,
				fmt.Sprintf("CREATE USER %s IDENTIFIED BY 'testpassword'", username)))
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, adminConn.Exec(cleanupCtx, "DROP USER IF EXISTS "+username))
			})
			if tt.profileSettings != "" {
				profile := username + "_profile"
				require.NoError(t, adminConn.Exec(ctx,
					fmt.Sprintf("CREATE SETTINGS PROFILE %s SETTINGS %s TO %s", profile, tt.profileSettings, username)))
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					require.NoError(t, adminConn.Exec(cleanupCtx, "DROP SETTINGS PROFILE IF EXISTS "+profile))
				})
			}

			conn, err := clickhouse.Open(&clickhouse.Options{
				Addr: []string{addr},
				Auth: clickhouse.Auth{Username: username, Password: "testpassword"},
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, conn.Close())
			})
			require.NoError(t, conn.Ping(ctx))

			err = CheckIfClickHouseCloudHasSharedMergeTreeEnabled(t.Context(), nopLogger{}, conn)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateClickHouseHost(t *testing.T) {
	tests := []struct {
		name           string
		host           string
		allowedDomains string
		wantErr        string
	}{
		{
			name:           "host matches allowed domain",
			host:           "myservice.clickhouse.cloud",
			allowedDomains: "clickhouse.cloud",
		},
		{
			name:           "host matches one of multiple allowed domains",
			host:           "myservice.example.com",
			allowedDomains: "clickhouse.cloud,example.com",
		},
		{
			name:           "host does not match allowed domain",
			host:           "myservice.evil.com",
			allowedDomains: "clickhouse.cloud",
			wantErr:        "invalid ClickHouse host domain",
		},
		{
			name:           "empty allowed domains permits any host",
			host:           "anything.example.com",
			allowedDomains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateClickHouseHost(t.Context(), tt.host, tt.allowedDomains)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateClusterShardingKey(t *testing.T) {
	tests := []struct {
		name           string
		cluster        string
		shardingKey    string
		sourceTable    string
		hasPrimaryKeys bool
		sortingKeys    []string
		wantErr        string
	}{
		{
			name:        "non-cluster mode always passes",
			cluster:     "",
			sourceTable: "db.no_pk",
		},
		{
			name:        "cluster with explicit sharding key passes",
			cluster:     "cicluster",
			shardingKey: "rand()",
			sourceTable: "db.no_pk",
		},
		{
			name:           "cluster with primary keys passes",
			cluster:        "cicluster",
			sourceTable:    "db.has_pk",
			hasPrimaryKeys: true,
		},
		{
			name:        "cluster with custom sorting columns passes",
			cluster:     "cicluster",
			sourceTable: "db.no_pk",
			sortingKeys: []string{"created_at"},
		},
		{
			name:        "cluster, no pk, no sharding key, no sorting → error",
			cluster:     "cicluster",
			sourceTable: "db.no_pk",
			wantErr:     "sharding_key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateClusterShardingKey(tt.cluster, tt.shardingKey, tt.sourceTable, tt.hasPrimaryKeys, tt.sortingKeys)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildPartitionByValidationQuery(t *testing.T) {
	t.Run("columns as dynamic placeholders", func(t *testing.T) {
		query := buildPartitionByValidationQuery("toYYYYMM(t)", []string{"id", "t"}, nil)
		require.Equal(t,
			"SELECT (toYYYYMM(t)) FROM (SELECT "+
				"CAST(NULL, 'Dynamic') AS `id`, "+
				"CAST(NULL, 'Dynamic') AS `t`) LIMIT 0",
			query)
	})

	t.Run("excluded columns are omitted", func(t *testing.T) {
		query := buildPartitionByValidationQuery("id % 2", []string{"id", "secret"}, []string{"secret"})
		require.Equal(t, "SELECT (id % 2) FROM (SELECT CAST(NULL, 'Dynamic') AS `id`) LIMIT 0", query)
	})
}

func TestCheckBucketGrantInValidatePeer(t *testing.T) {
	ctx := t.Context()
	addr := fmt.Sprintf("%s:%d", testutil.ClickHouseTestHost(), testutil.ClickHouseTestPort())
	adminConn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, adminConn.Close())
	})
	require.NoError(t, adminConn.Ping(ctx))

	database := "pkgch_" + strings.ToLower(common.RandomString(8))
	require.NoError(t, adminConn.Exec(ctx, "CREATE DATABASE "+QuoteIdentifier(database)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, adminConn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(database)))
	})

	// Create a test user to test grants with.
	username := "testuser_" + common.RandomString(8)
	require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY 'testpassword';", username)))
	require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("GRANT CREATE TABLE, ALTER TABLE, DROP TABLE, INSERT, SELECT ON "+
		"%s.* TO %s;", QuoteIdentifier(database), username)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, adminConn.Exec(cleanupCtx, "DROP USER IF EXISTS "+username))
	})

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: "testpassword",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	require.NoError(t, conn.Ping(ctx))

	// Staging bucket access methods to test.
	testCases := []struct {
		storageAccessType string
		fineScopedSyntax  bool
	}{
		{
			storageAccessType: "S3",
			fineScopedSyntax:  false,
		},
		{
			storageAccessType: "URL",
			fineScopedSyntax:  false,
		},
		{
			storageAccessType: "S3",
			fineScopedSyntax:  true,
		},
		{
			storageAccessType: "URL",
			fineScopedSyntax:  true,
		},
	}

	for _, tc := range testCases {
		// Expect an error
		err = ValidateClickHousePeer(
			t.Context(),
			nopLogger{},
			"clickhouse.cloud",
			"something.clickhouse.cloud",
			conn,
			tc.storageAccessType,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("failed to validate %s read grant", tc.storageAccessType))

		// Grant the appropriate privilege, then verify there's no error.
		if tc.fineScopedSyntax {
			require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("GRANT READ ON %s TO %s", tc.storageAccessType, username)))
		} else {
			require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("GRANT %s ON *.* TO %s", tc.storageAccessType, username)))
		}

		err = ValidateClickHousePeer(
			t.Context(),
			nopLogger{},
			"clickhouse.cloud",
			"something.clickhouse.cloud",
			conn,
			tc.storageAccessType,
		)
		require.NoError(t, err)

		// Drop the grant that was added earlier.
		if tc.fineScopedSyntax {
			require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("REVOKE READ ON %s FROM %s", tc.storageAccessType, username)))
		} else {
			require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf("REVOKE %s ON *.* FROM %s", tc.storageAccessType, username)))
		}
	}
}
