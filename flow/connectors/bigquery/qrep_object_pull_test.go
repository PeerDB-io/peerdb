package connbigquery

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildBigQueryExportSQL_ForSystemTimeAsOf(t *testing.T) {
	dsTable := datasetTable{dataset: "my_dataset", table: "my_table"}
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
	}
	snapshotTime := time.Date(2026, 8, 14, 12, 34, 56, 789000000, time.UTC)

	sql := buildBigQueryExportSQL(dsTable, schema, "gs://bucket/prefix", "my_dataset.my_table", snapshotTime)

	require.Contains(t, sql, "FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 12:34:56.789 UTC')")
	require.Contains(t, sql, "FROM `my_dataset`.`my_table` FOR SYSTEM_TIME AS OF")
	assert.Contains(t, sql, "uri='gs://bucket/prefix/my_dataset.my_table/*.parquet'")
	assert.Contains(t, sql, "`id`")
	assert.Contains(t, sql, "`name`")
}

func TestBuildBigQueryExportSQL_ConvertsNonUTCToUTC(t *testing.T) {
	dsTable := datasetTable{dataset: "ds", table: "tbl"}
	schema := bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}

	loc := time.FixedZone("UTC-5", -5*60*60)
	// 2026-08-14 10:00:00 -05:00 == 2026-08-14 15:00:00 UTC
	snapshotTime := time.Date(2026, 8, 14, 10, 0, 0, 0, loc)

	sql := buildBigQueryExportSQL(dsTable, schema, "gs://bucket", "tbl", snapshotTime)

	require.Contains(t, sql, "FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 15:00:00 UTC')")
}

func TestBuildBigQueryExportSQL_NoFractionalSeconds(t *testing.T) {
	dsTable := datasetTable{dataset: "ds", table: "tbl"}
	schema := bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}
	snapshotTime := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)

	sql := buildBigQueryExportSQL(dsTable, schema, "gs://bucket", "tbl", snapshotTime)

	// time.Format with ".999999" drops the fractional part entirely when it's zero.
	require.Contains(t, sql, "FOR SYSTEM_TIME AS OF TIMESTAMP('2026-08-14 00:00:00 UTC')")
}

func TestBuildBigQueryExportSQL_ColumnCasts(t *testing.T) {
	dsTable := datasetTable{dataset: "ds", table: "tbl"}
	schema := bigquery.Schema{
		{Name: "plain", Type: bigquery.StringFieldType},
		{Name: "payload", Type: bigquery.JSONFieldType},
		{Name: "geo", Type: bigquery.GeographyFieldType},
		{Name: "ts", Type: bigquery.DateTimeFieldType},
	}
	snapshotTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	sql := buildBigQueryExportSQL(dsTable, schema, "gs://bucket", "tbl", snapshotTime)

	assert.Contains(t, sql, "`plain`")
	assert.Contains(t, sql, "TO_JSON_STRING(`payload`) AS `payload`")
	assert.Contains(t, sql, "ST_AsText(`geo`) AS `geo`")
	assert.Contains(t, sql, "CAST(`ts` AS TIMESTAMP) AS `ts`")

	// Sanity check on the column order/joining.
	selectClause := sql[strings.Index(sql, "SELECT "):strings.Index(sql, " FROM ")]
	assert.Equal(t,
		"SELECT `plain`, TO_JSON_STRING(`payload`) AS `payload`, ST_AsText(`geo`) AS `geo`, CAST(`ts` AS TIMESTAMP) AS `ts`",
		selectClause,
	)
}

func TestBuildBigQueryExportSQL_ExportOptions(t *testing.T) {
	dsTable := datasetTable{dataset: "ds", table: "tbl"}
	schema := bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}
	snapshotTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

	sql := buildBigQueryExportSQL(dsTable, schema, "gs://bucket/prefix", "ds.tbl", snapshotTime)

	assert.Contains(t, sql, "format='PARQUET'")
	assert.Contains(t, sql, "compression='GZIP'")
	assert.Contains(t, sql, "overwrite=true")
}
