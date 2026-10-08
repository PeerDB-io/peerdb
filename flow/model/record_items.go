package model

import (
	"encoding/json"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type Items interface {
	json.Marshaler
	UpdateIfNotExists(Items) []string
	UpdateWithBaseRecord(BaseRecord)
	GetBytesByColName(string) ([]byte, error)
	ToJSONWithOptions(ToJSONOptions) (string, error)
	DeleteColName(string)
}

func ItemsToJSON(items Items) (string, error) {
	bytes, err := items.MarshalJSON()
	return string(bytes), err
}

// encoding/gob cannot encode unexported fields
type RecordItems struct {
	ColToVal map[string]types.QValue
}

func NewRecordItems(capacity int) RecordItems {
	return RecordItems{
		ColToVal: make(map[string]types.QValue, capacity),
	}
}

func (r RecordItems) AddColumn(col string, val types.QValue) {
	r.ColToVal[col] = val
}

func (r RecordItems) GetColumnValue(col string) types.QValue {
	return r.ColToVal[col]
}

// UpdateIfNotExists takes in a RecordItems as input and updates the values of the
// current RecordItems with the values from the input RecordItems for the columns
// that are present in the input RecordItems but not in the current RecordItems.
// We return the slice of col names that were updated.
func (r RecordItems) UpdateIfNotExists(input_ Items) []string {
	input := input_.(RecordItems)
	updatedCols := make([]string, 0, len(input.ColToVal))
	for col, val := range input.ColToVal {
		if _, ok := r.ColToVal[col]; !ok {
			r.ColToVal[col] = val
			updatedCols = append(updatedCols, col)
		}
	}
	return updatedCols
}

func (r RecordItems) UpdateWithBaseRecord(baseRecord BaseRecord) {
	r.AddColumn("_peerdb_origin_transaction_id", types.QValueUInt64{Val: baseRecord.GetTransactionID()})
	r.AddColumn("_peerdb_origin_checkpoint_id", types.QValueInt64{Val: baseRecord.GetCheckpointID()})
	r.AddColumn("_peerdb_origin_commit_time_nano", types.QValueInt64{Val: baseRecord.GetCommitTime().UnixNano()})
}

func (r RecordItems) GetValueByColName(colName string) (types.QValue, error) {
	val, ok := r.ColToVal[colName]
	if !ok {
		return nil, fmt.Errorf("column name %s not found", colName)
	}
	return val, nil
}

func (r RecordItems) GetBytesByColName(colName string) ([]byte, error) {
	val, err := r.GetValueByColName(colName)
	if err != nil {
		return nil, err
	}
	return fmt.Append(nil, val.Value()), nil
}

func (r RecordItems) Len() int {
	return len(r.ColToVal)
}

func (r RecordItems) ToJSONWithOptions(options ToJSONOptions) (string, error) {
	bytes, err := r.MarshalJSONWithOptions(options)
	return string(bytes), err
}

func (r RecordItems) MarshalJSON() ([]byte, error) {
	return r.MarshalJSONWithOptions(NewToJSONOptions(nil, true))
}

func (r RecordItems) DeleteColName(colName string) {
	delete(r.ColToVal, colName)
}
