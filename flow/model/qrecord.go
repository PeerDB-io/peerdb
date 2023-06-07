package model

import (
	"fmt"
	"time"

	"github.com/linkedin/goavro"
)

type QValueKind string

const (
	QValueKindInvalid QValueKind = "invalid"
	QValueKindFloat   QValueKind = "float"
	QValueKindInteger QValueKind = "int"
	QValueKindBoolean QValueKind = "bool"
	QValueKindArray   QValueKind = "array"
	QValueKindStruct  QValueKind = "struct"
	QValueKindString  QValueKind = "string"
	QValueKindETime   QValueKind = "extended_time"
)

type ExtendedTimeKindType string

const (
	DateTimeKindType ExtendedTimeKindType = "datetime"
	DateKindType     ExtendedTimeKindType = "date"
	TimeKindType     ExtendedTimeKindType = "time"
)

type ExtendedTime struct {
	time.Time
	NestedKind NestedKind
}

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	// DateTime represents the NestedKind for datetime objects, using RFC3339Nano format for timestamps
	DateTime = NestedKind{
		Type:   DateTimeKindType,
		Format: time.RFC3339Nano,
	}

	// Date represents the NestedKind for date objects, using "2006-01-02" format (equivalent to yyyy-mm-dd)
	Date = NestedKind{
		Type:   DateKindType,
		Format: "2006-01-02",
	}

	// Time represents the NestedKind for time objects, using "15:04:05.999999" format (equivalent to hh:mm:ss.ffffff)
	Time = NestedKind{
		Type:   TimeKindType,
		Format: "15:04:05.999999",
	}
)

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) (*ExtendedTime, error) {
	var nk NestedKind

	switch kindType {
	case DateTimeKindType:
		nk = DateTime
	case DateKindType:
		nk = Date
	case TimeKindType:
		nk = Time
	}

	if originalFormat != "" {
		nk.Format = originalFormat
	}

	return &ExtendedTime{
		Time:       t,
		NestedKind: nk,
	}, nil
}

type QValue struct {
	Kind  QValueKind
	Value interface{}
}

type QRecord map[string]QValue

func (q *QRecord) ToAvroCompatibleMap(nullableFields *map[string]bool) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	for key, qValue := range *q {
		switch qValue.Kind {
		case QValueKindETime:
			et, ok := qValue.Value.(*ExtendedTime)
			if !ok {
				return nil, fmt.Errorf("invalid ExtendedTime value")
			}
			var v interface{}
			switch et.NestedKind.Type {
			case DateTimeKindType:
				// Convert to timestamp (milliseconds since epoch)
				v = et.Time.UnixNano() / int64(time.Millisecond)
			case DateKindType:
				// Extract date in YYYY-MM-DD format
				v = et.Time.Format("2006-01-02")
			case TimeKindType:
				// Extract time in HH:MM:SS.ffffff format
				v = et.Time.Format("15:04:05.999999")
			default:
				return nil, fmt.Errorf("unsupported ExtendedTimeKindType: %s", et.NestedKind.Type)
			}
			m[key] = v
		default:
			// For all other types, we just copy the value and wrap with goavro.Union
			// if the field is nullable.
			if nullable, ok := (*nullableFields)[key]; ok && nullable {
				switch qValue.Kind {
				case QValueKindString:
					m[key] = goavro.Union("string", qValue.Value)
				case QValueKindFloat:
					m[key] = goavro.Union("float", qValue.Value)
				case QValueKindInteger:
					m[key] = goavro.Union("long", qValue.Value)
				case QValueKindBoolean:
					m[key] = goavro.Union("boolean", qValue.Value)
				case QValueKindArray:
					m[key] = goavro.Union("array", qValue.Value)
				case QValueKindStruct:
					m[key] = goavro.Union("map", qValue.Value)
				// TODO(kaushik/sai): Add more cases as needed
				default:
					return nil, fmt.Errorf("unsupported QValueKind: %s", qValue.Kind)
				}
			} else {
				m[key] = qValue.Value
			}
		}
	}

	return m, nil
}

// QRecordBatch holds a batch of QRecord objects.
type QRecordBatch struct {
	NumRecords uint32     // NumRecords represents the number of records in the batch.
	Records    []*QRecord // Records is a slice of pointers to QRecord objects.
}
