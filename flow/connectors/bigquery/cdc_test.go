package connbigquery

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestPollWindow(t *testing.T) {
	checkpoint := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	const safetyLag = time.Minute
	const maxQueryWindow = 24 * time.Hour

	t.Run("caps at maxQueryWindow past checkpoint when now is far ahead", func(t *testing.T) {
		now := checkpoint.Add(maxQueryWindow * 10)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		require.True(t, ok)
		assert.True(t, upper.Equal(checkpoint.Add(maxQueryWindow)))
	})

	t.Run("caps at safetyLag behind now when now is close", func(t *testing.T) {
		now := checkpoint.Add(time.Hour)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		require.True(t, ok)
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
	})

	t.Run("nothing new to scan when safety lag hasn't cleared", func(t *testing.T) {
		now := checkpoint.Add(safetyLag / 2)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		assert.False(t, ok)
		// upper is still reported (as now-safetyLag), just not usable, since it
		// doesn't move past checkpoint.
		assert.True(t, upper.Equal(now.Add(-safetyLag)))
		assert.False(t, upper.After(checkpoint))
	})

	t.Run("exactly at the boundary is not ok (upper must strictly move past checkpoint)", func(t *testing.T) {
		now := checkpoint.Add(safetyLag)
		upper, ok := pollWindow(checkpoint, now, safetyLag, maxQueryWindow)
		assert.False(t, ok)
		assert.True(t, upper.Equal(checkpoint))
	})
}

func TestBigQueryRowToRecordItems(t *testing.T) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTypeColumn, Type: bigquery.StringFieldType},
		{Name: bigQueryChangeTimestampColumn, Type: bigquery.TimestampFieldType},
		{Name: bigQueryChangeIsForUpdateColumn, Type: bigquery.BooleanFieldType},
	}
	qfields := make([]types.QField, len(schema))
	for i, f := range schema {
		qfields[i] = BigQueryFieldToQField(f)
	}
	row := []bigquery.Value{
		int64(1), "alice", bigQueryChangeTypeInsert, time.Now(), true,
	}

	items, err := bigQueryRowToRecordItems(schema, qfields, row)
	require.NoError(t, err)
	assert.Equal(t, types.QValueInt64{Val: 1}, items.GetColumnValue("id"))
	assert.Equal(t, types.QValueString{Val: "alice"}, items.GetColumnValue("name"))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTypeColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeTimestampColumn))
	assert.Nil(t, items.GetColumnValue(bigQueryChangeIsForUpdateColumn))
	assert.Len(t, items.ColToVal, 2)
}

func TestPullColumnNames(t *testing.T) {
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id"}, {Name: "secret"}, {Name: "name"},
		},
	}
	assert.Equal(t, []string{"id", "secret", "name"}, pullColumnNames(schema, nil))
	assert.Equal(t, []string{"id", "secret", "name"}, pullColumnNames(schema, map[string]struct{}{}))
	assert.Equal(t, []string{"id", "name"}, pullColumnNames(schema, map[string]struct{}{"secret": {}}))
}

func TestMissingSourceColumn(t *testing.T) {
	candidates := []string{"secret_column", "large_payload", "id"}

	err := &googleapi.Error{
		Code:    400,
		Message: "Unrecognized name: secret_column at [1:8]",
	}
	col, ok := missingSourceColumn(err, candidates)
	assert.True(t, ok)
	assert.Equal(t, "secret_column", col)

	col, ok = missingSourceColumn(fmt.Errorf("query failed: %w", err), candidates)
	assert.True(t, ok)
	assert.Equal(t, "secret_column", col)

	_, ok = missingSourceColumn(errors.New("some unrelated failure"), candidates)
	assert.False(t, ok)
	_, ok = missingSourceColumn(&googleapi.Error{Code: 404, Message: "secret_column"}, candidates)
	assert.False(t, ok)

	// Column not among candidates (e.g. already dropped, or a different query
	// entirely) must not be reported as newly missing.
	_, ok = missingSourceColumn(&googleapi.Error{Code: 400, Message: "Unrecognized name: other_col at [1:8]"}, candidates)
	assert.False(t, ok)
}

func TestEffectiveColumns(t *testing.T) {
	const retryAfter = time.Hour
	c := &BigQueryConnector{missingSourceColumns: map[string]map[string]time.Time{
		"ds.tbl": {"secret_column": time.Now()},
	}}
	columns := []string{"id", "secret_column", "large_payload"}

	// Still within the retry window: stays excluded.
	assert.Equal(t, []string{"id", "large_payload"}, c.effectiveColumns("ds.tbl", columns, retryAfter))
	// A different table was never affected.
	assert.Equal(t, columns, c.effectiveColumns("ds.other", columns, retryAfter))

	// Retry window elapsed: the customer may have re-added the column, so retry it.
	c.missingSourceColumns["ds.tbl"]["secret_column"] = time.Now().Add(-retryAfter - time.Minute)
	assert.Equal(t, columns, c.effectiveColumns("ds.tbl", columns, retryAfter))
}

func TestBuildPullQuery(t *testing.T) {
	assert.Equal(t,
		"SELECT `id`, `name` FROM APPENDS(TABLE `ds`.`tbl`, @start, @end)",
		buildPullQuery("APPENDS", "`ds`.`tbl`", []string{"id", "name"}, ""),
	)
	assert.Equal(t,
		"SELECT `id`, `name` FROM CHANGES(TABLE `ds`.`tbl`, @start, @end) ORDER BY `_CHANGE_TIMESTAMP`",
		buildPullQuery("CHANGES", "`ds`.`tbl`", []string{"id", "name"}, "`_CHANGE_TIMESTAMP`"),
	)
}
