package cdc_records

import (
	"crypto/rand"
	"math/big"
	"testing"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/stretchr/testify/require"
)

func getTimeForTesting(t *testing.T) time.Time {
	tv, err := time.Parse(time.RFC3339, "2021-08-01T08:02:00Z")
	require.NoError(t, err)

	millisToAdd := 716
	tv = tv.Add(time.Millisecond * time.Duration(millisToAdd))

	microSecondsToAdd := 506
	tv = tv.Add(time.Microsecond * time.Duration(microSecondsToAdd))

	return tv
}

func getRatForTesting(t *testing.T) *big.Rat {
	return big.NewRat(123456789, 987654321)
}

func genKeyAndRec(t *testing.T) (model.TableWithPkey, model.Record) {
	t.Helper()

	pkeyColVal := make([]byte, 32)
	_, err := rand.Read(pkeyColVal)
	require.NoError(t, err)

	tv := getTimeForTesting(t)
	rv := getRatForTesting(t)

	key := model.TableWithPkey{
		TableName:  "test_src_tbl",
		PkeyColVal: [32]byte(pkeyColVal),
	}
	rec := &model.InsertRecord{
		SourceTableName:      "test_src_tbl",
		DestinationTableName: "test_dst_tbl",
		CheckPointID:         1,
		CommitID:             2,
		Items: &model.RecordItems{
			ColToValIdx: map[string]int{
				"id": 0,
				"ts": 1,
				"rv": 2,
			},
			Values: []qvalue.QValue{
				{
					Kind:  qvalue.QValueKindInt64,
					Value: 1,
				},
				{
					Kind:  qvalue.QValueKindTime,
					Value: tv,
				},
				{
					Kind:  qvalue.QValueKindNumeric,
					Value: rv,
				},
			},
		},
	}
	return key, rec
}

func TestSingleRecord(t *testing.T) {
	t.Parallel()
	cdcRecordsStore := NewCDCRecordsStore("test_single_record")
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	key, rec := genKeyAndRec(t)
	err := cdcRecordsStore.Set(key, rec)
	require.NoError(t, err)
	// should not spill into DB
	require.Equal(t, 1, len(cdcRecordsStore.inMemoryRecords))
	require.Nil(t, cdcRecordsStore.pebbleDB)

	reck, ok, err := cdcRecordsStore.Get(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, reck)

	require.NoError(t, cdcRecordsStore.Close())
}

func TestRecordsTillSpill(t *testing.T) {
	t.Parallel()
	cdcRecordsStore := NewCDCRecordsStore("test_records_till_spill")
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	// add records upto set limit
	for i := 0; i < 10; i++ {
		key, rec := genKeyAndRec(t)
		err := cdcRecordsStore.Set(key, rec)
		require.NoError(t, err)
		require.Equal(t, i+1, len(cdcRecordsStore.inMemoryRecords))
		require.Nil(t, cdcRecordsStore.pebbleDB)
	}

	// this record should be spilled to DB
	key, rec := genKeyAndRec(t)
	err := cdcRecordsStore.Set(key, rec)
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

func TestTimeAndRatEncoding(t *testing.T) {
	t.Parallel()

	cdcRecordsStore := NewCDCRecordsStore("test_time_encoding")
	cdcRecordsStore.numRecordsSwitchThreshold = 0

	key, rec := genKeyAndRec(t)
	err := cdcRecordsStore.Set(key, rec)
	require.NoError(t, err)

	retreived, ok, err := cdcRecordsStore.Get(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, retreived)

	_, err = retreived.GetItems().ToJSON()
	require.NoError(t, err)

	require.NoError(t, cdcRecordsStore.Close())
}
