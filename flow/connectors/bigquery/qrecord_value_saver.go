package connbigquery

import (
	"fmt"
	"math/big"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/google/uuid"
)

type QRecordValueSaver struct {
	ColumnNames []string
	Record      *model.QRecord
	PartitionID string
	RunID       int64
}

// RatToBigQueryNumeric converts a *big.Rat to a decimal string compatible with
// BigQuery's NUMERIC type.
//
// BigQuery's NUMERIC type supports large-scale fixed-point numbers with up to
// 38 digits of precision and 9 digits of scale. This function converts a *big.Rat
// to a decimal string that respects these limits.
//
// The function uses *big.Rat's FloatString method with 9 as the argument, which
// converts the *big.Rat to a string that represents a floating-point number with
// 9 digits after the decimal point. The resulting string can be inserted into a
// NUMERIC field in BigQuery.
//
// Parameters:
// rat: The *big.Rat to convert. This should represent a decimal number with up to
//
//	38 digits of precision and 9 digits of scale.
//
// Returns:
// A string representing the *big.Rat as a decimal number with up to 38 digits
// of precision and 9 digits of scale. This string can be inserted into a NUMERIC
// field in BigQuery.
func RatToBigQueryNumeric(rat *big.Rat) string {
	// Convert the *big.Rat to a decimal string with 9 digits of scale
	return rat.FloatString(9) // 9 is the scale of the NUMERIC type
}

func (q QRecordValueSaver) Save() (map[string]bigquery.Value, string, error) {
	bqValues := make(map[string]bigquery.Value, q.Record.NumEntries)

	for i, v := range q.Record.Entries {
		k := q.ColumnNames[i]
		if v.Value == nil {
			bqValues[k] = nil
			continue
		}

		switch v.Kind {
		case qvalue.QValueKindFloat32:
			val, ok := v.Value.(float32)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to float64", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindFloat64:
			val, ok := v.Value.(float64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to float64", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindInt16:
			val, ok := v.Value.(int16)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to int16", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindInt32:
			val, ok := v.Value.(int32)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to int32", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindInt64:
			val, ok := v.Value.(int64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to int64", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindBoolean:
			val, ok := v.Value.(bool)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to bool", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindString:
			val, ok := v.Value.(string)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to string", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindTimestamp, qvalue.QValueKindDate, qvalue.QValueKindTime:
			var err error
			bqValues[k], err = v.GoTimeConvert()
			if err != nil {
				return nil, "", fmt.Errorf("failed to convert parse %v into time.Time", v)
			}

		case qvalue.QValueKindNumeric:
			val, ok := v.Value.(*big.Rat)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to *big.Rat", v.Value)
			}

			bqValues[k] = RatToBigQueryNumeric(val)

		case qvalue.QValueKindBytes, qvalue.QValueKindBit:
			val, ok := v.Value.([]byte)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to []byte", v.Value)
			}
			bqValues[k] = val

		case qvalue.QValueKindUUID:
			val, ok := v.Value.([16]byte)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to string", v.Value)
			}
			uuidVal := uuid.UUID(val)
			bqValues[k] = uuidVal.String()

		default:
			// Skip invalid QValueKind, but log the type for debugging
			fmt.Printf("[bigquery] Invalid QValueKind: %v\n", v.Kind)
		}
	}

	// add partition id to the map
	bqValues["PartitionID"] = q.PartitionID

	// add run id to the map
	bqValues["RunID"] = q.RunID

	// log the bigquery values
	// fmt.Printf("BigQuery Values: %v\n", bqValues)

	return bqValues, "", nil
}
