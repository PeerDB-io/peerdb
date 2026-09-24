//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
)

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

	restrictedUser := "capacity_user_" + strings.ToLower(common.RandomString(8))
	restrictedPassword := common.RandomString(16)
	require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf(
		"CREATE USER %s IDENTIFIED WITH plaintext_password BY %s",
		QuoteIdentifier(restrictedUser), QuoteLiteral(restrictedPassword),
	)))
	t.Cleanup(func() {
		require.NoError(t, adminConn.Exec(context.Background(), "DROP USER IF EXISTS "+QuoteIdentifier(restrictedUser)))
	})
	require.NoError(t, adminConn.Exec(ctx, fmt.Sprintf(
		"GRANT ALL ON %s.* TO %s", QuoteIdentifier(database), QuoteIdentifier(restrictedUser),
	)))
	restrictedConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: restrictedUser,
			Password: restrictedPassword,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, restrictedConn.Close())
	})
	require.NoError(t, restrictedConn.Ping(ctx))

	t.Run("missing system table privileges skips validation", func(t *testing.T) {
		require.NoError(t, ValidateTableCapacity(
			t.Context(), nopLogger{}, restrictedConn, tableNames[:maxTables], 1, false,
		))
	})

	require.NoError(t, adminConn.Exec(ctx,
		"GRANT SELECT ON system.server_settings TO "+QuoteIdentifier(restrictedUser),
	))
	t.Run("missing metrics privileges skips validation", func(t *testing.T) {
		require.NoError(t, ValidateTableCapacity(
			t.Context(), nopLogger{}, restrictedConn, tableNames[:maxTables], 1, false,
		))
	})

	require.NoError(t, adminConn.Exec(ctx,
		"GRANT SELECT ON system.metrics TO "+QuoteIdentifier(restrictedUser),
	))

	// Exercise enforcement with the same user once both required grants are present.
	t.Run("one missing table fits", func(t *testing.T) {
		require.NoError(t, ValidateTableCapacity(t.Context(), nopLogger{}, restrictedConn, tableNames[:1], 0, false))
	})

	t.Run("missing tables and raw table exceed limit", func(t *testing.T) {
		err := ValidateTableCapacity(t.Context(), nopLogger{}, restrictedConn, tableNames[:maxTables], 1, false)
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
		err := ValidateTableCapacity(t.Context(), nopLogger{}, restrictedConn, tableNames, 0, false)
		var capacityErr *TableCapacityExceededError
		require.ErrorAs(t, err, &capacityErr)
		require.Equal(t, maxTables, capacityErr.MaxTables)
		require.Equal(t, maxTables, capacityErr.RequiredAdditionalTables)
	})

	t.Run("existing resync table reserves transient slot", func(t *testing.T) {
		err := ValidateTableCapacity(t.Context(), nopLogger{}, restrictedConn, tableNames, 0, true)
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
