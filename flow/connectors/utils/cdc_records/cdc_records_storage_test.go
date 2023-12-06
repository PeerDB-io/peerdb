//nolint:stylecheck
package cdc_records

import (
	"crypto/rand"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
)

type CDCRecordStorageTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func (s *CDCRecordStorageTestSuite) genKeyAndRec() (model.TableWithPkey, model.Record) {
	pkeyColVal := make([]byte, 0, 32)
	_, err := rand.Read(pkeyColVal)
	s.NoError(err)

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
			ColToValIdx: map[string]int{"id": 0},
			Values: []*qvalue.QValue{{
				Kind:  qvalue.QValueKindInt64,
				Value: 1,
			}},
		},
	}
	return key, rec
}

func (s *CDCRecordStorageTestSuite) TestSingleRecord() {
	cdcRecordsStore := NewCDCRecordsStore("test_single_record")
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	key, rec := s.genKeyAndRec()
	err := cdcRecordsStore.Set(key, rec)
	s.NoError(err)
	// should not spill into DB
	s.Equal(1, len(cdcRecordsStore.inMemoryRecords))
	s.Nil(cdcRecordsStore.pebbleDB)

	reck, ok, err := cdcRecordsStore.Get(key)
	s.NoError(err)
	s.True(ok)
	s.Equal(rec, reck)

	s.NoError(cdcRecordsStore.Close())
}

func (s *CDCRecordStorageTestSuite) TestRecordsTillSpill() {
	cdcRecordsStore := NewCDCRecordsStore("test_records_till_spill")
	cdcRecordsStore.numRecordsSwitchThreshold = 10

	// add records upto set limit
	for i := 0; i < 10; i++ {
		key, rec := s.genKeyAndRec()
		err := cdcRecordsStore.Set(key, rec)
		s.NoError(err)
		s.Equal(i+1, len(cdcRecordsStore.inMemoryRecords))
		s.Nil(cdcRecordsStore.pebbleDB)
	}

	// this record should be spilled to DB
	key, rec := s.genKeyAndRec()
	err := cdcRecordsStore.Set(key, rec)
	s.NoError(err)
	_, ok := cdcRecordsStore.inMemoryRecords[key]
	s.False(ok)
	s.NotNil(cdcRecordsStore.pebbleDB)

	reck, ok, err := cdcRecordsStore.Get(key)
	s.NoError(err)
	s.True(ok)
	s.Equal(rec, reck)

	s.NoError(cdcRecordsStore.Close())
}
