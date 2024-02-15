package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

// encoding/gob cannot encode unexported fields
type RecordItems struct {
	ColToValIdx map[string]int
	Values      []qvalue.QValue
}

func NewRecordItems(capacity int) *RecordItems {
	return &RecordItems{
		ColToValIdx: make(map[string]int, capacity),
		Values:      make([]qvalue.QValue, 0, capacity),
	}
}

func NewRecordItemWithData(cols []string, val []qvalue.QValue) *RecordItems {
	recordItem := NewRecordItems(len(cols))
	for i, col := range cols {
		recordItem.ColToValIdx[col] = len(recordItem.Values)
		recordItem.Values = append(recordItem.Values, val[i])
	}
	return recordItem
}

func (r *RecordItems) AddColumn(col string, val qvalue.QValue) {
	if idx, ok := r.ColToValIdx[col]; ok {
		r.Values[idx] = val
	} else {
		r.ColToValIdx[col] = len(r.Values)
		r.Values = append(r.Values, val)
	}
}

func (r *RecordItems) GetColumnValue(col string) qvalue.QValue {
	if idx, ok := r.ColToValIdx[col]; ok {
		return r.Values[idx]
	}
	return qvalue.QValue{}
}

// UpdateIfNotExists takes in a RecordItems as input and updates the values of the
// current RecordItems with the values from the input RecordItems for the columns
// that are present in the input RecordItems but not in the current RecordItems.
// We return the slice of col names that were updated.
func (r *RecordItems) UpdateIfNotExists(input *RecordItems) []string {
	updatedCols := make([]string, 0)
	for col, idx := range input.ColToValIdx {
		if _, ok := r.ColToValIdx[col]; !ok {
			r.ColToValIdx[col] = len(r.Values)
			r.Values = append(r.Values, input.Values[idx])
			updatedCols = append(updatedCols, col)
		}
	}
	return updatedCols
}

func (r *RecordItems) GetValueByColName(colName string) (qvalue.QValue, error) {
	idx, ok := r.ColToValIdx[colName]
	if !ok {
		return qvalue.QValue{}, fmt.Errorf("column name %s not found", colName)
	}
	return r.Values[idx], nil
}

func (r *RecordItems) Len() int {
	return len(r.Values)
}

func (r *RecordItems) toMap(hstoreAsJSON bool) (map[string]interface{}, error) {
	if r.ColToValIdx == nil {
		return nil, errors.New("colToValIdx is nil")
	}

	jsonStruct := make(map[string]interface{}, len(r.ColToValIdx))
	for col, idx := range r.ColToValIdx {
		v := r.Values[idx]
		if v.Value == nil {
			jsonStruct[col] = nil
			continue
		}

		var err error
		switch v.Kind {
		case qvalue.QValueKindBit, qvalue.QValueKindBytes:
			bitVal, ok := v.Value.([]byte)
			if !ok {
				return nil, errors.New("expected []byte value")
			}

			// convert to binary string because
			// json.Marshal stores byte arrays as
			// base64
			binStr := ""
			for _, b := range bitVal {
				binStr += fmt.Sprintf("%08b", b)
			}

			jsonStruct[col] = binStr
		case qvalue.QValueKindQChar:
			ch, ok := v.Value.(uint8)
			if !ok {
				return nil, fmt.Errorf("expected \"char\" value for column %s for %T", col, v.Value)
			}

			jsonStruct[col] = string(ch)
		case qvalue.QValueKindString, qvalue.QValueKindJSON:
			strVal, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string value for column %s for %T", col, v.Value)
			}

			if len(strVal) > 15*1024*1024 {
				jsonStruct[col] = ""
			} else {
				jsonStruct[col] = strVal
			}
		case qvalue.QValueKindHStore:
			hstoreVal, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string value for hstore column %s for value %T", col, v.Value)
			}

			if !hstoreAsJSON {
				jsonStruct[col] = hstoreVal
			} else {
				jsonVal, err := hstore_util.ParseHstore(hstoreVal)
				if err != nil {
					return nil, fmt.Errorf("unable to convert hstore column %s to json for value %T", col, v.Value)
				}

				if len(jsonVal) > 15*1024*1024 {
					jsonStruct[col] = ""
				} else {
					jsonStruct[col] = jsonVal
				}
			}

		case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindDate,
			qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
			jsonStruct[col], err = v.GoTimeConvert()
			if err != nil {
				return nil, err
			}
		case qvalue.QValueKindArrayDate:
			dateArr, ok := v.Value.([]time.Time)
			if !ok {
				return nil, errors.New("expected []time.Time value")
			}
			formattedDateArr := make([]string, 0, len(dateArr))
			for _, val := range dateArr {
				formattedDateArr = append(formattedDateArr, val.Format("2006-01-02"))
			}
			jsonStruct[col] = formattedDateArr
		case qvalue.QValueKindNumeric:
			bigRat, ok := v.Value.(*big.Rat)
			if !ok {
				return nil, errors.New("expected *big.Rat value")
			}

			if bigRat == nil {
				jsonStruct[col] = nil
				continue
			}
			jsonStruct[col] = bigRat.FloatString(100)
		case qvalue.QValueKindFloat64:
			floatVal, ok := v.Value.(float64)
			if !ok {
				return nil, errors.New("expected float64 value")
			}
			if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = floatVal
			}
		case qvalue.QValueKindFloat32:
			floatVal, ok := v.Value.(float32)
			if !ok {
				return nil, errors.New("expected float32 value")
			}
			if math.IsNaN(float64(floatVal)) || math.IsInf(float64(floatVal), 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = floatVal
			}
		case qvalue.QValueKindArrayFloat64:
			floatArr, ok := v.Value.([]float64)
			if !ok {
				return nil, errors.New("expected []float64 value")
			}

			nullableFloatArr := make([]interface{}, 0, len(floatArr))
			for _, val := range floatArr {
				if math.IsNaN(val) || math.IsInf(val, 0) {
					nullableFloatArr = append(nullableFloatArr, nil)
				} else {
					nullableFloatArr = append(nullableFloatArr, val)
				}
			}
			jsonStruct[col] = nullableFloatArr
		case qvalue.QValueKindArrayFloat32:
			floatArr, ok := v.Value.([]float32)
			if !ok {
				return nil, errors.New("expected []float32 value")
			}
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
			jsonStruct[col] = v.Value
		}
	}

	return jsonStruct, nil
}

// a separate method like gives flexibility
// for us to handle some data types differently
func (r *RecordItems) ToJSONWithOptions(options *ToJSONOptions) (string, error) {
	return r.ToJSONWithOpts(options)
}

func (r *RecordItems) ToJSON() (string, error) {
	unnestCols := make([]string, 0)
	return r.ToJSONWithOpts(NewToJSONOptions(unnestCols, true))
}

func (r *RecordItems) ToJSONWithOpts(opts *ToJSONOptions) (string, error) {
	jsonStruct, err := r.toMap(opts.HStoreAsJSON)
	if err != nil {
		return "", err
	}

	for col, idx := range r.ColToValIdx {
		v := r.Values[idx]
		if v.Kind == qvalue.QValueKindJSON {
			if _, ok := opts.UnnestColumns[col]; ok {
				var unnestStruct map[string]interface{}
				err := json.Unmarshal([]byte(v.Value.(string)), &unnestStruct)
				if err != nil {
					return "", err
				}

				for k, v := range unnestStruct {
					jsonStruct[k] = v
				}
				delete(jsonStruct, col)
			}
		}
	}

	jsonBytes, err := json.Marshal(jsonStruct)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}
