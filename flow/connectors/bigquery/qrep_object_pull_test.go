package connbigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func TestBuildBigQueryExportSQL(t *testing.T) {
	tests := []struct {
		name                  string
		dsTable               datasetTable
		schema                bigquery.Schema
		snapshotStagingPath   string
		sourceTableIdentifier string
		snapshotTime          time.Time
		watermarkColumn       string
		expected              string
	}{
		{
			name:    "basic",
			dsTable: datasetTable{dataset: "my_dataset", table: "my_table"},
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "name", Type: bigquery.StringFieldType},
			},
			snapshotStagingPath:   "gs://bucket/prefix",
			sourceTableIdentifier: "my_dataset.my_table",
			snapshotTime:          time.Date(2026, 8, 14, 12, 34, 56, 789000000, time.UTC),
			expected: "EXPORT DATA OPTIONS(uri='gs://bucket/prefix/my_dataset.my_table/*.parquet', format='PARQUET', " +
				"compression='GZIP', overwrite=true) AS SELECT `id`, `name` FROM `my_dataset`.`my_table` " +
				"FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 12:34:56.789 UTC')",
		},
		{
			name:                  "non-UTC snapshot time is converted to UTC",
			dsTable:               datasetTable{dataset: "ds", table: "tbl"},
			schema:                bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 10, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60)), // == 2026-08-14 15:00:00 UTC
			expected: "EXPORT DATA OPTIONS(uri='gs://bucket/tbl/*.parquet', format='PARQUET', " +
				"compression='GZIP', overwrite=true) AS " +
				"SELECT `id` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 15:00:00 UTC')",
		},
		{
			name:                  "no fractional seconds are dropped from the literal",
			dsTable:               datasetTable{dataset: "ds", table: "tbl"},
			schema:                bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC),
			expected: "EXPORT DATA OPTIONS(uri='gs://bucket/tbl/*.parquet', format='PARQUET', " +
				"compression='GZIP', overwrite=true) AS " +
				"SELECT `id` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 00:00:00 UTC')",
		},
		{
			name:    "JSON, Geography, and DateTime columns are cast for Parquet export",
			dsTable: datasetTable{dataset: "ds", table: "tbl"},
			schema: bigquery.Schema{
				{Name: "plain", Type: bigquery.StringFieldType},
				{Name: "payload", Type: bigquery.JSONFieldType},
				{Name: "geo", Type: bigquery.GeographyFieldType},
				{Name: "ts", Type: bigquery.DateTimeFieldType},
			},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
			expected: "EXPORT DATA OPTIONS(uri='gs://bucket/tbl/*.parquet', format='PARQUET', " +
				"compression='GZIP', overwrite=true) AS " +
				"SELECT `plain`, TO_JSON_STRING(`payload`) AS `payload`, ST_AsText(`geo`) AS `geo`, " +
				"CAST(`ts` AS TIMESTAMP) AS `ts` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 12:00:00 UTC')",
		},
		{
			name:    "watermark column filters instead of FOR SYSTEM_TIME AS OF",
			dsTable: datasetTable{dataset: "ds", table: "tbl"},
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "updated_at", Type: bigquery.TimestampFieldType},
			},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
			watermarkColumn:       "updated_at",
			expected: "EXPORT DATA OPTIONS(uri='gs://bucket/tbl/*.parquet', format='PARQUET', " +
				"compression='GZIP', overwrite=true) AS " +
				"SELECT `id`, `updated_at` FROM `ds`.`tbl` WHERE TIMESTAMP(`updated_at`) <= TIMESTAMP('2026-08-14 12:00:00 UTC')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := buildBigQueryExportSQL(
				tt.dsTable, tt.schema, tt.snapshotStagingPath, tt.sourceTableIdentifier, tt.snapshotTime, tt.watermarkColumn)
			require.Equal(t, tt.expected, sql)
		})
	}
}
