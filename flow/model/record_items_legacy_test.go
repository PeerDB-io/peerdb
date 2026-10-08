package model

// Frozen pre-token implementation: independent oracle for compatibility tests.
import (
	"encoding/json"
	"fmt"
	"maps"
	"math"

	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (r RecordItems) toMap(opts ToJSONOptions) (map[string]any, error) {
	jsonStruct := make(map[string]any, len(r.ColToVal))
	for col, qv := range r.ColToVal {
		if qv == nil {
			jsonStruct[col] = nil
			continue
		}

		switch v := qv.(type) {
		case types.QValueUUID:
			jsonStruct[col] = v.Val
		case types.QValueQChar:
			jsonStruct[col] = string(v.Val)
		case types.QValueString:
			strVal := v.Val
			if opts.ClearValuesOverBytes > 0 && len(strVal) > opts.ClearValuesOverBytes {
				jsonStruct[col] = ""
			} else {
				jsonStruct[col] = strVal
			}
		case types.QValueJSON:
			if opts.ClearValuesOverBytes > 0 && len(v.Val) > opts.ClearValuesOverBytes {
				jsonStruct[col] = "{}"
			} else if _, ok := opts.UnnestColumns[col]; ok {
				var unnestStruct map[string]any
				if err := json.Unmarshal([]byte(v.Val), &unnestStruct); err != nil {
					return nil, err
				}

				maps.Copy(jsonStruct, unnestStruct)
			} else {
				jsonStruct[col] = v.Val
			}
		case types.QValueHStore:
			hstoreVal := v.Val

			if !opts.HStoreAsJSON {
				jsonStruct[col] = hstoreVal
			} else {
				jsonVal, err := datatypes.ParseHstore(hstoreVal)
				if err != nil {
					return nil, fmt.Errorf("unable to convert hstore column %s to json for value %T: %w", col, v, err)
				}
				if opts.ClearValuesOverBytes > 0 && len(jsonVal) > opts.ClearValuesOverBytes {
					jsonStruct[col] = ""
				} else {
					jsonStruct[col] = jsonVal
				}
			}

		case types.QValueTimestamp:
			jsonStruct[col] = v.Val.Format("2006-01-02 15:04:05.999999")
		case types.QValueTimestampTZ:
			jsonStruct[col] = v.Val.Format("2006-01-02 15:04:05.999999-0700")
		case types.QValueDate:
			jsonStruct[col] = v.Val.Format("2006-01-02")
		case types.QValueTime:
			jsonStruct[col] = types.FormatExtendedTimeDuration(v.Val)
		case types.QValueTimeTZ:
			jsonStruct[col] = types.FormatExtendedTimeDuration(v.Val)
		case types.QValueArrayDate:
			dateArr := v.Val
			formattedDateArr := make([]string, 0, len(dateArr))
			for _, val := range dateArr {
				formattedDateArr = append(formattedDateArr, val.Format("2006-01-02"))
			}
			jsonStruct[col] = formattedDateArr
		case types.QValueArrayTime:
			timeArr := v.Val
			formattedTimeArr := make([]string, 0, len(timeArr))
			for _, val := range timeArr {
				formattedTimeArr = append(formattedTimeArr, types.FormatExtendedTimeDuration(val))
			}
			jsonStruct[col] = formattedTimeArr
		case types.QValueNumeric:
			jsonStruct[col] = v.Val.String()
		case types.QValueArrayNumeric:
			numericArr := v.Val
			strArr := make([]any, 0, len(numericArr))
			for _, val := range numericArr {
				strArr = append(strArr, val.String())
			}
			jsonStruct[col] = strArr
		case types.QValueFloat64:
			if math.IsNaN(v.Val) || math.IsInf(v.Val, 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = v.Val
			}
		case types.QValueFloat32:
			if math.IsNaN(float64(v.Val)) || math.IsInf(float64(v.Val), 0) {
				jsonStruct[col] = nil
			} else {
				jsonStruct[col] = v.Val
			}
		case types.QValueArrayFloat64:
			floatArr := v.Val
			nullableFloatArr := make([]any, 0, len(floatArr))
			for _, val := range floatArr {
				if math.IsNaN(val) || math.IsInf(val, 0) {
					nullableFloatArr = append(nullableFloatArr, nil)
				} else {
					nullableFloatArr = append(nullableFloatArr, val)
				}
			}
			jsonStruct[col] = nullableFloatArr
		case types.QValueArrayFloat32:
			floatArr := v.Val
			nullableFloatArr := make([]any, 0, len(floatArr))
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
