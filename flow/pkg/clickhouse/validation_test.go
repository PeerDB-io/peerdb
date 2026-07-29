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

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func init() {
	testutil.LoadEnv()
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
