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

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

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

func TestExceptClause(t *testing.T) {
	assert.Empty(t, exceptClause(nil))
	assert.Empty(t, exceptClause(map[string]struct{}{}))
	assert.Equal(t, " EXCEPT (`a`, `b`)", exceptClause(map[string]struct{}{"b": {}, "a": {}}))
}

func TestMissingExceptColumns(t *testing.T) {
	candidates := map[string]struct{}{"secret_column": {}, "large_payload": {}, "id": {}}

	err := &googleapi.Error{
		Code:    400,
		Message: "Column secret_column in SELECT * EXCEPT list does not exist at [1:18]",
	}
	assert.Equal(t, map[string]struct{}{"secret_column": {}}, missingExceptColumns(err, candidates))
	assert.Equal(t, map[string]struct{}{"secret_column": {}}, missingExceptColumns(fmt.Errorf("query failed: %w", err), candidates))

	assert.Empty(t, missingExceptColumns(errors.New("some unrelated failure"), candidates))
	assert.Empty(t, missingExceptColumns(&googleapi.Error{Code: 404, Message: "secret_column"}, candidates))
	assert.Empty(t, missingExceptColumns(&googleapi.Error{Code: 400, Message: "Unrecognized name: secret_column"}, candidates))

	// Excluding both "id" and "user_id": losing user_id must not also flag "id".
	idCandidates := map[string]struct{}{"id": {}, "user_id": {}}
	userIDErr := &googleapi.Error{
		Code:    400,
		Message: "Column user_id in SELECT * EXCEPT list does not exist at [1:18]",
	}
	assert.Equal(t, map[string]struct{}{"user_id": {}}, missingExceptColumns(userIDErr, idCandidates))

	// A column literally named after a word in the error's fixed boilerplate must
	// not be flagged by some other column's error.
	boilerplateCandidates := map[string]struct{}{"at": {}, "list": {}}
	assert.Empty(t, missingExceptColumns(err, boilerplateCandidates))
}

func TestEffectiveExclude(t *testing.T) {
	c := &BigQueryConnector{droppedExcludeColumns: map[string]map[string]struct{}{
		"ds.tbl": {"secret_column": {}},
	}}
	exclude := map[string]struct{}{"secret_column": {}, "large_payload": {}}

	assert.Equal(t, map[string]struct{}{"large_payload": {}}, c.effectiveExclude("ds.tbl", exclude))
	assert.Equal(t, exclude, c.effectiveExclude("ds.other", exclude))
}

func TestBuildPullQuery(t *testing.T) {
	assert.Equal(t,
		"SELECT * FROM APPENDS(TABLE `ds`.`tbl`, @start, @end)",
		buildPullQuery("APPENDS", "`ds`.`tbl`", nil, ""),
	)
	assert.Equal(t,
		"SELECT * EXCEPT (`secret`) FROM APPENDS(TABLE `ds`.`tbl`, @start, @end)",
		buildPullQuery("APPENDS", "`ds`.`tbl`", map[string]struct{}{"secret": {}}, ""),
	)
	assert.Equal(t,
		"SELECT * EXCEPT (`secret`) FROM CHANGES(TABLE `ds`.`tbl`, @start, @end) ORDER BY `_CHANGE_TIMESTAMP`",
		buildPullQuery("CHANGES", "`ds`.`tbl`", map[string]struct{}{"secret": {}}, "`_CHANGE_TIMESTAMP`"),
	)
}
