package connbigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/model"
)

type QRecordValueSaver struct {
	ColumnNames []string
	Record      *model.QRecord
	PartitionID string
	RunID       int64
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
			val, ok := v.Value.(string)
			if !ok {
				return nil, "", fmt.Errorf("failed to convert %v to float64", v.Value)
			}
			bqValues[k] = val

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
