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
		expected              string
	}{
		{
			name:                  "basic",
			dsTable:               datasetTable{dataset: "my_dataset", table: "my_table"},
			schema:                bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType}},
			snapshotStagingPath:   "gs://bucket/prefix",
			sourceTableIdentifier: "my_dataset.my_table",
			snapshotTime:          time.Date(2026, 8, 14, 12, 34, 56, 789000000, time.UTC),
			expected: "EXPORT DATA OPTIONS(\n\t\t\turi='gs://bucket/prefix/my_dataset.my_table/*.parquet',\n\t\t\tformat='PARQUET',\n\t\t\tcompression='GZIP',\n\t\t\toverwrite=true\n\t\t) AS\n\t\tSELECT `id`, `name` FROM `my_dataset`.`my_table` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 12:34:56.789 UTC')",
		},
		{
			name:                  "non-UTC snapshot time is converted to UTC",
			dsTable:               datasetTable{dataset: "ds", table: "tbl"},
			schema:                bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 10, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60)), // == 2026-08-14 15:00:00 UTC
			expected: "EXPORT DATA OPTIONS(\n\t\t\turi='gs://bucket/tbl/*.parquet',\n\t\t\tformat='PARQUET',\n\t\t\tcompression='GZIP',\n\t\t\toverwrite=true\n\t\t) AS\n\t\tSELECT `id` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 15:00:00 UTC')",
		},
		{
			name:                  "no fractional seconds are dropped from the literal",
			dsTable:               datasetTable{dataset: "ds", table: "tbl"},
			schema:                bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC),
			expected: "EXPORT DATA OPTIONS(\n\t\t\turi='gs://bucket/tbl/*.parquet',\n\t\t\tformat='PARQUET',\n\t\t\tcompression='GZIP',\n\t\t\toverwrite=true\n\t\t) AS\n\t\tSELECT `id` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 00:00:00 UTC')",
		},
		{
			name:                  "JSON, Geography, and DateTime columns are cast for Parquet export",
			dsTable:               datasetTable{dataset: "ds", table: "tbl"},
			schema: bigquery.Schema{
				{Name: "plain", Type: bigquery.StringFieldType},
				{Name: "payload", Type: bigquery.JSONFieldType},
				{Name: "geo", Type: bigquery.GeographyFieldType},
				{Name: "ts", Type: bigquery.DateTimeFieldType},
			},
			snapshotStagingPath:   "gs://bucket",
			sourceTableIdentifier: "tbl",
			snapshotTime:          time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
			expected: "EXPORT DATA OPTIONS(\n\t\t\turi='gs://bucket/tbl/*.parquet',\n\t\t\tformat='PARQUET',\n\t\t\tcompression='GZIP',\n\t\t\toverwrite=true\n\t\t) AS\n\t\tSELECT `plain`, TO_JSON_STRING(`payload`) AS `payload`, ST_AsText(`geo`) AS `geo`, CAST(`ts` AS TIMESTAMP) AS `ts` FROM `ds`.`tbl` FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 12:00:00 UTC')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := buildBigQueryExportSQL(tt.dsTable, tt.schema, tt.snapshotStagingPath, tt.sourceTableIdentifier, tt.snapshotTime)
			require.Equal(t, tt.expected, sql)
		})
	}
}
