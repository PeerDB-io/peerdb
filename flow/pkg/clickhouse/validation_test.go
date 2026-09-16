package clickhouse

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

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
			name:           "empty domain entry does not allow unrelated host",
			host:           "myservice.evil.com",
			allowedDomains: "clickhouse.cloud,",
			wantErr:        "invalid ClickHouse host domain",
		},
		{
			name:           "whitespace and empty domain entries are ignored",
			host:           "myservice.example.com",
			allowedDomains: " clickhouse.cloud, example.com, ",
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
