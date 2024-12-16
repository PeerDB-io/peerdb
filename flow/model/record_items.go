package model

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type Items interface {
	json.Marshaler
	UpdateIfNotExists(Items) []string
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
	ColToVal map[string]qvalue.QValue
}

func NewRecordItems(capacity int) RecordItems {
	return RecordItems{
		ColToVal: make(map[string]qvalue.QValue, capacity),
	}
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

func (r RecordItems) GetValueByColName(colName string) (qvalue.QValue, error) {
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
	return []byte(fmt.Sprint(val.Value())), nil
}

func (r RecordItems) Len() int {
	return len(r.ColToVal)
}

func (r RecordItems) toMap(opts ToJSONOptions) (map[string]interface{}, error) {
	jsonStruct := make(map[string]interface{}, len(r.ColToVal))
	for col, qv := range r.ColToVal {
		if qv == nil {
			jsonStruct[col] = nil
			continue
		}

		switch v := qv.(type) {
		case qvalue.QValueUUID:
			jsonStruct[col] = v.Val
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
				jsonStruct[col] = "{}"
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

			if !opts.HStoreAsJSON {
				jsonStruct[col] = hstoreVal
			} else {
				jsonVal, err := datatypes.ParseHstore(hstoreVal)
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

func (r RecordItems) MarshalJSON() ([]byte, error) {
	return r.MarshalJSONWithOptions(NewToJSONOptions(nil, true))
}

func (r RecordItems) MarshalJSONWithOptions(opts ToJSONOptions) ([]byte, error) {
	jsonStruct, err := r.toMap(opts)
	if err != nil {
		return nil, err
	}

	return json.Marshal(jsonStruct)
}

func (r RecordItems) DeleteColName(colName string) {
	delete(r.ColToVal, colName)
}
