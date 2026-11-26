package utils

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var (
	timeForTesting    = time.Duration(18342121716506000)
	decimalForTesting = decimal.New(9876543210, 123)
)

func cdcStoreTestEnv() map[string]string {
	return map[string]string{
		"PEERDB_CDC_DISK_SPILL_RECORDS_THRESHOLD":     "1000000",
		"PEERDB_CDC_DISK_SPILL_MEM_PERCENT_THRESHOLD": "-1",
	}
}

func genKeyAndRec(t *testing.T) (model.TableWithPkey, model.Record[model.RecordItems]) {
	t.Helper()

	pkeyColVal := make([]byte, 32)
	_, err := rand.Read(pkeyColVal)
	require.NoError(t, err)

	tv := timeForTesting
	rv := decimalForTesting

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
			ColToVal: map[string]types.QValue{
				"id": types.QValueInt64{Val: 1},
				"ts": types.QValueTime{Val: tv},
				"rv": types.QValueNumeric{Val: rv},
			},
		},
	}
	return key, rec
}

func TestSingleRecord(t *testing.T) {
	t.Parallel()
	cdcRecordsStore, err := NewCDCStore[model.RecordItems](t.Context(), cdcStoreTestEnv(), "test_single_record")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	key, rec := genKeyAndRec(t)
	require.NoError(t, cdcRecordsStore.Set(key, rec))
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
	cdcRecordsStore, err := NewCDCStore[model.RecordItems](t.Context(), cdcStoreTestEnv(), "test_records_till_spill")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	// add records upto set limit
	for i := 1; i <= 10; i++ {
		key, rec := genKeyAndRec(t)
		err := cdcRecordsStore.Set(key, rec)
		require.NoError(t, err)
		require.Len(t, cdcRecordsStore.inMemoryRecords, i)
		require.Nil(t, cdcRecordsStore.pebbleDB)
	}

	// this record should be spilled to DB
	key, rec := genKeyAndRec(t)
	require.NoError(t, cdcRecordsStore.Set(key, rec))
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

	cdcRecordsStore, err := NewCDCStore[model.RecordItems](t.Context(), cdcStoreTestEnv(), "test_time_encoding")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	key, rec := genKeyAndRec(t)
	require.NoError(t, cdcRecordsStore.Set(key, rec))

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

	cdcRecordsStore, err := NewCDCStore[model.RecordItems](t.Context(), cdcStoreTestEnv(), "test_time_encoding")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	key, rec := genKeyAndRec(t)
	require.NoError(t, cdcRecordsStore.Set(model.TableWithPkey{}, rec))

	retreived, ok, err := cdcRecordsStore.Get(key)
	require.Nil(t, retreived)
	require.NoError(t, err)
	require.False(t, ok)

	require.Equal(t, 1, int(cdcRecordsStore.numRecords.Load()))

	require.NoError(t, cdcRecordsStore.Close())
}

// An empty binary value comes back from the disk spill with a nil Val (gob doesn't keep empty slices), while a NULL
// stays QValueNull. A Postgres destination's raw-table JSON (NilBytesAsEmpty) still writes it as empty, for the
// items of inserts and both images of updates.
func TestEmptyBytesAfterSpill(t *testing.T) {
	t.Parallel()

	cdcRecordsStore, err := NewCDCStore[model.RecordItems](t.Context(), cdcStoreTestEnv(), "test_empty_bytes_spill")
	require.NoError(t, err)
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	items := func() model.RecordItems {
		return model.RecordItems{ColToVal: map[string]types.QValue{
			"empty": types.QValueBytes{Val: []byte{}},
			"null":  types.QValueNull(types.QValueKindBytes),
		}}
	}
	insertKey, _ := genKeyAndRec(t)
	updateKey, _ := genKeyAndRec(t)
	require.NoError(t, cdcRecordsStore.Set(insertKey, &model.InsertRecord[model.RecordItems]{Items: items()}))
	require.NoError(t, cdcRecordsStore.Set(updateKey, &model.UpdateRecord[model.RecordItems]{
		OldItems: items(), NewItems: items(),
	}))
	require.NotNil(t, cdcRecordsStore.pebbleDB)

	inserted, ok, err := cdcRecordsStore.Get(insertKey)
	require.NoError(t, err)
	require.True(t, ok)
	updated, ok, err := cdcRecordsStore.Get(updateKey)
	require.NoError(t, err)
	require.True(t, ok)
	update := updated.(*model.UpdateRecord[model.RecordItems])

	require.Nil(t, inserted.GetItems().GetColumnValue("empty").(types.QValueBytes).Val, "the spill loses the empty slice")
	for _, spilled := range []model.RecordItems{inserted.GetItems(), update.OldItems, update.NewItems} {
		got, err := spilled.ToJSONWithOptions(model.ToJSONOptions{NilBytesAsEmpty: true})
		require.NoError(t, err)
		require.JSONEq(t, `{"empty":"","null":null}`, got)
	}

	require.NoError(t, cdcRecordsStore.Close())
}
