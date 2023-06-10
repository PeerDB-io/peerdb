package connbigquery

import (
	"fmt"
	"math/big"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/model"
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
		switch v.Kind {
		case model.QValueKindFloat:
			val, ok := v.Value.(float64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to float64", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindInteger:
			val, ok := v.Value.(int64)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to int64", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindBoolean:
			val, ok := v.Value.(bool)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to bool", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindString:
			val, ok := v.Value.(string)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to string", v.Value)
			}
			bqValues[k] = val

		case model.QValueKindETime:
			val, ok := v.Value.(*model.ExtendedTime)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to ExtendedTime", v.Value)
			}
			bqValues[k] = val.Time

		case model.QValueKindNumeric:
			val, ok := v.Value.(*big.Rat)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to *big.Rat", v.Value)
			}

			bqValues[k] = RatToBigQueryNumeric(val)

		case model.QValueKindBytes:
			val, ok := v.Value.([]byte)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to []byte", v.Value)
			}
			bqValues[k] = val

		default:
			// Skip invalid QValueKind
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
