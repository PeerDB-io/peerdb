package clickhouse

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

type tableRow struct {
	name      string
	engine    string
	totalRows uint64
}

type mockRows struct {
	driver.Rows
	rows  []tableRow
	index int
}

func (m *mockRows) Next() bool {
	return m.index < len(m.rows)
}

func (m *mockRows) Scan(dest ...any) error {
	row := m.rows[m.index]
	m.index++
	*dest[0].(*string) = row.name
	*dest[1].(*string) = row.engine
	*dest[2].(*uint64) = row.totalRows
	return nil
}

func (m *mockRows) Close() error { return nil }
func (m *mockRows) Err() error   { return nil }

type mockConn struct {
	driver.Conn
	rows *mockRows
}

func (m *mockConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return m.rows, nil
}

func TestCheckIfTablesEmptyAndEngine(t *testing.T) {
	tests := []struct {
		name                   string
		wantErr                string
		host                   string
		allowedDomains         string
		rows                   []tableRow
		tables                 []string
		initialSnapshotEnabled bool
		checkForCloudSMT       bool
		allowNonEmpty          bool
	}{
		{
			name:    "view rejected",
			rows:    []tableRow{{name: "test_view", engine: "View", totalRows: 0}},
			tables:  []string{"test_view"},
			wantErr: "destination table can not be a view",
		},
		{
			name:    "materialized view rejected",
			rows:    []tableRow{{name: "test_mv", engine: "MaterializedView", totalRows: 0}},
			tables:  []string{"test_mv"},
			wantErr: "destination table can not be a view",
		},
		{
			name:                   "non-empty table with snapshot rejected",
			rows:                   []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 100}},
			tables:                 []string{"test_table"},
			initialSnapshotEnabled: true,
			wantErr:                "table test_table exists and is not empty",
		},
		{
			name:                   "non-empty table allowed when allowNonEmpty",
			rows:                   []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 100}},
			tables:                 []string{"test_table"},
			initialSnapshotEnabled: true,
			allowNonEmpty:          true,
		},
		{
			name:   "acceptable engine passes",
			rows:   []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 0}},
			tables: []string{"test_table"},
		},
		{
			name:             "shared engine passes cloud SMT check",
			rows:             []tableRow{{name: "test_table", engine: "SharedReplacingMergeTree", totalRows: 0}},
			tables:           []string{"test_table"},
			checkForCloudSMT: true,
		},
		{
			name:             "non-shared engine fails cloud SMT check",
			rows:             []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 0}},
			tables:           []string{"test_table"},
			checkForCloudSMT: true,
			wantErr:          "table test_table exists and does not use SharedMergeTree engine",
		},
		{
			name:   "empty table list passes",
			tables: nil,
		},
		{
			name:           "host matches allowed domain",
			host:           "myservice.clickhouse.cloud",
			allowedDomains: "clickhouse.cloud",
			rows:           []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 0}},
			tables:         []string{"test_table"},
		},
		{
			name:           "host matches one of multiple allowed domains",
			host:           "myservice.example.com",
			allowedDomains: "clickhouse.cloud,example.com",
			rows:           []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 0}},
			tables:         []string{"test_table"},
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
			rows:           []tableRow{{name: "test_table", engine: "ReplacingMergeTree", totalRows: 0}},
			tables:         []string{"test_table"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.host != "" || tt.allowedDomains != "" {
				if err := ValidateClickHouseHost(context.Background(), tt.host, tt.allowedDomains); err != nil {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
			}
			conn := &mockConn{rows: &mockRows{rows: tt.rows}}
			err := CheckIfTablesEmptyAndEngine(
				context.Background(), nopLogger{}, conn,
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
