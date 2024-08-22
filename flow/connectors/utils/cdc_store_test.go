package utils

import (
	"context"
	"crypto/rand"
	"log/slog"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func getTimeForTesting(t *testing.T) time.Time {
	t.Helper()
	tv, err := time.Parse(time.RFC3339, "2021-08-01T08:02:00Z")
	require.NoError(t, err)

	millisToAdd := 716
	tv = tv.Add(time.Millisecond * time.Duration(millisToAdd))

	microSecondsToAdd := 506
	tv = tv.Add(time.Microsecond * time.Duration(microSecondsToAdd))

	return tv
}

func getDecimalForTesting(t *testing.T) decimal.Decimal {
	t.Helper()
	return decimal.New(9876543210, 123)
}

func genKeyAndRec(t *testing.T) (model.TableWithPkey, model.Record[model.RecordItems]) {
	t.Helper()

	pkeyColVal := make([]byte, 32)
	_, err := rand.Read(pkeyColVal)
	require.NoError(t, err)

	tv := getTimeForTesting(t)
	rv := getDecimalForTesting(t)

	key := model.TableWithPkey{
		TableName:  "test_src_tbl",
		PkeyColVal: [32]byte(pkeyColVal),
	}
	rec := &model.InsertRecord[model.RecordItems]{
		BaseRecord: model.BaseRecord{
			CheckpointID:   1,
			CommitTimeNano: time.Now().UnixNano(),
		},
		SourceTableName:      "test_src_tbl",
		DestinationTableName: "test_dst_tbl",
		CommitID:             2,
		Items: model.RecordItems{
			ColToVal: map[string]qvalue.QValue{
				"id": qvalue.QValueInt64{Val: 1},
				"ts": qvalue.QValueTime{Val: tv},
				"rv": qvalue.QValueNumeric{Val: rv},
			},
		},
	}
	return key, rec
}

func TestSingleRecord(t *testing.T) {
	t.Parallel()
	cdcRecordsStore, err := NewCDCStore[model.RecordItems](context.Background(), nil, "test_single_record")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	key, rec := genKeyAndRec(t)
	err = cdcRecordsStore.Set(slog.Default(), key, rec)
	require.NoError(t, err)
	// should not spill into DB
	require.Len(t, cdcRecordsStore.inMemoryRecords, 1)
	require.Nil(t, cdcRecordsStore.pebbleDB)

	reck, ok, err := cdcRecordsStore.Get(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, reck)

	require.NoError(t, cdcRecordsStore.Close())
}

func TestRecordsTillSpill(t *testing.T) {
	t.Parallel()
	cdcRecordsStore, err := NewCDCStore[model.RecordItems](context.Background(), nil, "test_records_till_spill")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	// add records upto set limit
	for i := 1; i <= 10; i++ {
		key, rec := genKeyAndRec(t)
		err := cdcRecordsStore.Set(slog.Default(), key, rec)
		require.NoError(t, err)
		require.Len(t, cdcRecordsStore.inMemoryRecords, i)
		require.Nil(t, cdcRecordsStore.pebbleDB)
	}

	// this record should be spilled to DB
	key, rec := genKeyAndRec(t)
	err = cdcRecordsStore.Set(slog.Default(), key, rec)
	require.NoError(t, err)
	_, ok := cdcRecordsStore.inMemoryRecords[key]
	require.False(t, ok)
	require.NotNil(t, cdcRecordsStore.pebbleDB)

	reck, ok, err := cdcRecordsStore.Get(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, reck)

	require.NoError(t, cdcRecordsStore.Close())
}

func TestTimeAndDecimalEncoding(t *testing.T) {
	t.Parallel()

	cdcRecordsStore, err := NewCDCStore[model.RecordItems](context.Background(), nil, "test_time_encoding")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	key, rec := genKeyAndRec(t)
	err = cdcRecordsStore.Set(slog.Default(), key, rec)
	require.NoError(t, err)

	retreived, ok, err := cdcRecordsStore.Get(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, retreived)

	_, err = model.ItemsToJSON(retreived.GetItems())
	require.NoError(t, err)

	require.NoError(t, cdcRecordsStore.Close())
}

func TestNullKeyDoesntStore(t *testing.T) {
	t.Parallel()

	cdcRecordsStore, err := NewCDCStore[model.RecordItems](context.Background(), nil, "test_time_encoding")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	key, rec := genKeyAndRec(t)
	err = cdcRecordsStore.Set(slog.Default(), model.TableWithPkey{}, rec)
	require.NoError(t, err)

	retreived, ok, err := cdcRecordsStore.Get(key)
	require.Nil(t, retreived)
	require.NoError(t, err)
	require.False(t, ok)

	require.Equal(t, 1, cdcRecordsStore.Len())

	require.NoError(t, cdcRecordsStore.Close())
}
