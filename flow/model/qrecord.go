package model

import (
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
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
	QValueKindNumeric QValueKind = "numeric"
	QValueKindBytes   QValueKind = "bytes"
	QValueKindUUID    QValueKind = "uuid"
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

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
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
	}
}

type QValue struct {
	Kind  QValueKind
	Value interface{}
}

type QRecord struct {
	NumEntries int
	Entries    []QValue
}

// create a new QRecord with n values
func NewQRecord(n int) *QRecord {
	return &QRecord{
		NumEntries: n,
		Entries:    make([]QValue, n),
	}
}

// Sets the value at the given index
func (q *QRecord) Set(idx int, value QValue) {
	q.Entries[idx] = value
}

func (q *QRecord) ToAvroCompatibleMap(
	nullableFields *map[string]bool,
	colNames []string,
) (map[string]interface{}, error) {
	m := map[string]interface{}{}

	for idx, qValue := range q.Entries {
		key := colNames[idx]
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
				timestampMillis := et.Time.UnixNano() / int64(time.Millisecond)
				// v = map[string]interface{}{"long.timestamp-millis": timestampMillis}
				v = timestampMillis
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
					m[key] = goavro.Union("double", qValue.Value)
				case QValueKindInteger:
					m[key] = goavro.Union("long", qValue.Value)
				case QValueKindBoolean:
					m[key] = goavro.Union("boolean", qValue.Value)
				case QValueKindArray:
					m[key] = goavro.Union("array", qValue.Value)
				case QValueKindStruct:
					m[key] = goavro.Union("map", qValue.Value)
				case QValueKindNumeric:
					if strNum, ok := qValue.Value.(string); ok {
						//nolint:gosec
						num, ok := new(big.Rat).SetString(strNum)
						if !ok {
							return nil, fmt.Errorf("invalid Numeric value")
						}

						// If the field is nullable, wrap the *big.Rat in a union
						if nullable, ok := (*nullableFields)[key]; ok && nullable {
							m[key] = goavro.Union("bytes.decimal", num)
						} else {
							m[key] = num
						}
					} else {
						return nil, fmt.Errorf("invalid Numeric value")
					}
				case QValueKindBytes:
					if byteData, ok := qValue.Value.([]byte); ok {
						m[key] = goavro.Union("bytes", byteData)
					} else {
						return nil, fmt.Errorf("invalid Bytes value")
					}
				case QValueKindUUID:
					if byteData, ok := qValue.Value.([16]byte); ok {
						// Convert [16]byte to UUID
						u, err := uuid.FromBytes(byteData[:])
						if err != nil {
							return nil, fmt.Errorf("conversion of invalid UUID value: %v", err)
						}

						// Convert UUID to string
						uuidString := u.String()

						m[key] = goavro.Union("string", uuidString)
					} else {
						// log the value for debugging
						// fmt.Printf("value type: %T\n", qValue.Value)
						// fmt.Printf("invalid UUID value: %v\n", qValue.Value)
						return nil, fmt.Errorf("invalid UUID value")
					}
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
	NumRecords  uint32     // NumRecords represents the number of records in the batch.
	Records     []*QRecord // Records is a slice of pointers to QRecord objects.
	ColumnNames []string   // ColumnNames is a slice of column names.
}
