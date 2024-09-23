package model

import (
	"encoding/json"
	"fmt"

	"github.com/PeerDB-io/peer-flow/shared"
)

// encoding/gob cannot encode unexported fields
type PgItems struct {
	ColToVal map[string][]byte
}

func NewPgItems(capacity int) PgItems {
	return PgItems{
		ColToVal: make(map[string][]byte, capacity),
	}
}

func (r PgItems) AddColumn(col string, val []byte) {
	r.ColToVal[col] = val
}

func (r PgItems) GetColumnValue(col string) []byte {
	return r.ColToVal[col]
}

// UpdateIfNotExists takes in a RecordItems as input and updates the values of the
// current RecordItems with the values from the input RecordItems for the columns
// that are present in the input RecordItems but not in the current RecordItems.
// We return the slice of col names that were updated.
func (r PgItems) UpdateIfNotExists(input_ Items) []string {
	input := input_.(PgItems)
	updatedCols := make([]string, 0, len(input.ColToVal))
	for col, val := range input.ColToVal {
		if _, ok := r.ColToVal[col]; !ok {
			r.ColToVal[col] = val
			updatedCols = append(updatedCols, col)
		}
	}
	return updatedCols
}

func (r PgItems) GetBytesByColName(colName string) ([]byte, error) {
	val, ok := r.ColToVal[colName]
	if !ok {
		return nil, fmt.Errorf("column name %s not found", colName)
	}
	return val, nil
}

func (r PgItems) Len() int {
	return len(r.ColToVal)
}

func (r PgItems) ToJSONWithOptions(options ToJSONOptions) (string, error) {
	bytes, err := r.MarshalJSON()
	return shared.UnsafeFastReadOnlyBytesToString(bytes), err
}

func (r PgItems) ToJSON() (string, error) {
	return r.ToJSONWithOptions(NewToJSONOptions(nil, true))
}

func (r PgItems) MarshalJSON() ([]byte, error) {
	jsonStruct := make(map[string]interface{}, len(r.ColToVal))
	for col, bytes := range r.ColToVal {
		if bytes == nil {
			jsonStruct[col] = nil
		} else {
			jsonStruct[col] = shared.UnsafeFastReadOnlyBytesToString(bytes)
		}
	}

	return json.Marshal(jsonStruct)
}

func (r PgItems) DeleteColName(colName string) {
	delete(r.ColToVal, colName)
}
