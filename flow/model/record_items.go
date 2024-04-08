package model

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/google/uuid"

	hstore_util "github.com/PeerDB-io/peer-flow/datatypes/hstore"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// encoding/gob cannot encode unexported fields
type RecordItems struct {
	ColToVal map[string]qvalue.QValue
}

func NewRecordItems(capacity int) RecordItems {
	return RecordItems{
		ColToVal: make(map[string]qvalue.QValue, capacity),
	}
}

func NewRecordItemWithData(cols []string, val []qvalue.QValue) RecordItems {
	recordItem := NewRecordItems(len(cols))
	for i, col := range cols {
		recordItem.ColToVal[col] = val[i]
	}
	return recordItem
}

func (r RecordItems) AddColumn(col string, val qvalue.QValue) {
	r.ColToVal[col] = val
}

func (r RecordItems) GetColumnValue(col string) qvalue.QValue {
	return r.ColToVal[col]
}

// UpdateIfNotExists takes in a RecordItems as input and updates the values of the
// current RecordItems with the values from the input RecordItems for the columns
// that are present in the input RecordItems but not in the current RecordItems.
// We return the slice of col names that were updated.
func (r RecordItems) UpdateIfNotExists(input RecordItems) []string {
	updatedCols := make([]string, 0)
	for col, val := range input.ColToVal {
		if _, ok := r.ColToVal[col]; !ok {
			r.ColToVal[col] = val
			updatedCols = append(updatedCols, col)
		}
	}
	return updatedCols
}

func (r RecordItems) GetValueByColName(colName string) (qvalue.QValue, error) {
	val, ok := r.ColToVal[colName]
	if !ok {
		return nil, fmt.Errorf("column name %s not found", colName)
	}
	return val, nil
}

func (r RecordItems) Len() int {
	return len(r.ColToVal)
}

func (r RecordItems) toMap(hstoreAsJSON bool, opts ToJSONOptions) (map[string]interface{}, error) {
	jsonStruct := make(map[string]interface{}, len(r.ColToVal))
	for col, qv := range r.ColToVal {
		if qv == nil {
			jsonStruct[col] = nil
			continue
		}

		switch v := qv.(type) {
		case qvalue.QValueBit:
			bitVal := v.Val

			// convert to binary string because
			// json.Marshal stores byte arrays as
			// base64
			binStr := ""
			for _, b := range bitVal {
				binStr += fmt.Sprintf("%08b", b)
			}

			jsonStruct[col] = binStr
		case qvalue.QValueBytes:
			bitVal := v.Val

			// convert to binary string because
			// json.Marshal stores byte arrays as
			// base64
			binStr := ""
			for _, b := range bitVal {
				binStr += fmt.Sprintf("%08b", b)
			}

			jsonStruct[col] = binStr
		case qvalue.QValueUUID:
			jsonStruct[col] = uuid.UUID(v.Val)
		case qvalue.QValueQChar:
			jsonStruct[col] = string(v.Val)
		case qvalue.QValueString:
			strVal := v.Val

			if len(strVal) > 15*1024*1024 {
				jsonStruct[col] = ""
			} else {
				jsonStruct[col] = strVal
			}
		case qvalue.QValueJSON:
			if len(v.Val) > 15*1024*1024 {
				jsonStruct[col] = ""
			} else if _, ok := opts.UnnestColumns[col]; ok {
				var unnestStruct map[string]interface{}
				err := json.Unmarshal([]byte(v.Val), &unnestStruct)
				if err != nil {
					return nil, err
				}

				for k, v := range unnestStruct {
					jsonStruct[k] = v
				}
			} else {
				jsonStruct[col] = v.Val
			}
		case qvalue.QValueHStore:
			hstoreVal := v.Val

			if !hstoreAsJSON {
				jsonStruct[col] = hstoreVal
			} else {
				jsonVal, err := hstore_util.ParseHstore(hstoreVal)
				if err != nil {
					return nil, fmt.Errorf("unable to convert hstore column %s to json for value %T: %w", col, v, err)
				}

				if len(jsonVal) > 15*1024*1024 {
					jsonStruct[col] = ""
				} else {
					jsonStruct[col] = jsonVal
				}
			}

		case qvalue.QValueTimestamp:
			jsonStruct[col] = v.Val.Format("2006-01-02 15:04:05.999999")
		case qvalue.QValueTimestampTZ:
			jsonStruct[col] = v.Val.Format("2006-01-02 15:04:05.999999-0700")
		case qvalue.QValueDate:
			jsonStruct[col] = v.Val.Format("2006-01-02")
		case qvalue.QValueTime:
			jsonStruct[col] = v.Val.Format("15:04:05.999999")
		case qvalue.QValueTimeTZ:
			jsonStruct[col] = v.Val.Format("15:04:05.999999")
		case qvalue.QValueArrayDate:
			dateArr := v.Val
			formattedDateArr := make([]string, 0, len(dateArr))
			for _, val := range dateArr {
				formattedDateArr = append(formattedDateArr, val.Format("2006-01-02"))
			}
			jsonStruct[col] = formattedDateArr
		case qvalue.QValueNumeric:
			jsonStruct[col] = v.Val.String()
		case qvalue.QValueFloat64:
			if math.IsNaN(v.Val) || math.IsInf(v.Val, 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = v.Val
			}
		case qvalue.QValueFloat32:
			if math.IsNaN(float64(v.Val)) || math.IsInf(float64(v.Val), 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = v.Val
			}
		case qvalue.QValueArrayFloat64:
			floatArr := v.Val
			nullableFloatArr := make([]interface{}, 0, len(floatArr))
			for _, val := range floatArr {
				if math.IsNaN(val) || math.IsInf(val, 0) {
					nullableFloatArr = append(nullableFloatArr, nil)
				} else {
					nullableFloatArr = append(nullableFloatArr, val)
				}
			}
			jsonStruct[col] = nullableFloatArr
		case qvalue.QValueArrayFloat32:
			floatArr := v.Val
			nullableFloatArr := make([]interface{}, 0, len(floatArr))
			for _, val := range floatArr {
				if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
					nullableFloatArr = append(nullableFloatArr, nil)
				} else {
					nullableFloatArr = append(nullableFloatArr, val)
				}
			}
			jsonStruct[col] = nullableFloatArr

		default:
			jsonStruct[col] = v.Value()
		}
	}

	return jsonStruct, nil
}

func (r RecordItems) ToJSONWithOptions(options ToJSONOptions) (string, error) {
	bytes, err := r.MarshalJSONWithOptions(options)
	return string(bytes), err
}

func (r RecordItems) ToJSON() (string, error) {
	return r.ToJSONWithOptions(NewToJSONOptions(nil, true))
}

func (r RecordItems) MarshalJSON() ([]byte, error) {
	return r.MarshalJSONWithOptions(NewToJSONOptions(nil, true))
}

func (r RecordItems) MarshalJSONWithOptions(opts ToJSONOptions) ([]byte, error) {
	jsonStruct, err := r.toMap(opts.HStoreAsJSON, opts)
	if err != nil {
		return nil, err
	}

	return json.Marshal(jsonStruct)
}
