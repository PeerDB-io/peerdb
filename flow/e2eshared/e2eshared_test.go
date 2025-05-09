package e2eshared

import (
	"errors"
	"os"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

func TestReadMissingFileToBytes(t *testing.T) {
	if _, err := ReadFileToBytes("missing"); !errors.Is(err, os.ErrNotExist) {
		t.Error("ReadFileToBytes expected to fail on missing files")
	}
}

func TestInequalRecordCountsInequal(t *testing.T) {
	if CheckQRecordEquality(t,
		[]qvalue.QValue{qvalue.QValueNull(qvalue.QValueKindString), qvalue.QValueNull(qvalue.QValueKindString)},
		[]qvalue.QValue{qvalue.QValueNull(qvalue.QValueKindString)},
	) {
		t.Error("2 records should not be equal to 1 record")
	}
}

func TestInequalRecordSchemasInequal(t *testing.T) {
	if CheckEqualRecordBatches(t,
		&model.QRecordBatch{Schema: qvalue.QRecordSchema{
			Fields: []qvalue.QField{{Name: "name"}},
		}},
		&model.QRecordBatch{Schema: qvalue.QRecordSchema{
			Fields: []qvalue.QField{{Name: "different"}},
		}},
	) {
		t.Error("schemas with differing column names should be non-equal")
	}

	if !CheckEqualRecordBatches(t,
		&model.QRecordBatch{Schema: qvalue.QRecordSchema{
			Fields: []qvalue.QField{{Name: "name"}},
		}},
		&model.QRecordBatch{Schema: qvalue.QRecordSchema{
			Fields: []qvalue.QField{{Name: "name"}},
		}},
	) {
		t.Error("empty batches with same schema should be equal")
	}
}

func TestNilBatchEquality(t *testing.T) {
	if !CheckEqualRecordBatches(t, nil, nil) {
		t.Error("two nil batches should be equal")
	}

	if CheckEqualRecordBatches(t, nil, &model.QRecordBatch{}) {
		t.Error("nil batch should not be equal to non-nil batch")
	}

	if CheckEqualRecordBatches(t, &model.QRecordBatch{}, nil) {
		t.Error("non-nil batch should not be equal to nil batch")
	}
}
