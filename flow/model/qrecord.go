package model

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
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

// equals checks if two QRecords are identical.
func (q *QRecord) equals(other *QRecord) bool {
	// First check simple attributes
	if q.NumEntries != other.NumEntries {
		return false
	}

	// Compare each entry
	for i, entry := range q.Entries {
		otherEntry := other.Entries[i]

		// For each value kind, we will delegate to a helper function
		switch entry.Kind {
		case QValueKindUUID:
			if !compareUUID(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindFloat:
			if !compareFloat(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindInteger:
			if !compareInteger(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindBoolean:
			if !compareBoolean(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindBytes:
			if !compareBytes(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindNumeric:
			if !compareNumeric(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindString:
			if !compareString(entry.Value, otherEntry.Value) {
				return false
			}
		case QValueKindETime:
			if !compareETime(entry.Value, otherEntry.Value) {
				return false
			}
		// Add cases for other QValueKind values as needed
		default:
			return false
		}
	}

	return true
}

func compareETime(value1, value2 interface{}) bool {
	et1, ok1 := value1.(*ExtendedTime)
	et2, ok2 := value2.(*ExtendedTime)

	if !ok1 || !ok2 {
		return false
	}

	t1 := et1.Time.UnixNano() / int64(time.Millisecond)
	t2 := et2.Time.UnixNano() / int64(time.Millisecond)

	return t1 == t2
}

func compareUUID(value1, value2 interface{}) bool {
	uuid1, ok1 := getUUID(value1)
	uuid2, ok2 := getUUID(value2)

	return ok1 && ok2 && uuid1 == uuid2
}

func compareFloat(value1, value2 interface{}) bool {
	float1, ok1 := getFloat(value1)
	float2, ok2 := getFloat(value2)

	return ok1 && ok2 && float1 == float2
}

func compareInteger(value1, value2 interface{}) bool {
	int1, ok1 := getInt(value1)
	int2, ok2 := getInt(value2)
	return ok1 && ok2 && int1 == int2
}

func compareBoolean(value1, value2 interface{}) bool {
	bool1, ok1 := value1.(bool)
	bool2, ok2 := value2.(bool)

	return ok1 && ok2 && bool1 == bool2
}

func compareBytes(value1, value2 interface{}) bool {
	bytes1, ok1 := getBytes(value1)
	bytes2, ok2 := getBytes(value2)

	return ok1 && ok2 && bytes.Equal(bytes1, bytes2)
}

func compareNumeric(value1, value2 interface{}) bool {
	rat1, ok1 := getRat(value1)
	rat2, ok2 := getRat(value2)

	return ok1 && ok2 && rat1.Cmp(rat2) == 0
}

func compareString(value1, value2 interface{}) bool {
	str1, ok1 := value1.(string)
	str2, ok2 := value2.(string)

	return ok1 && ok2 && str1 == str2
}

func getBytes(v interface{}) ([]byte, bool) {
	switch value := v.(type) {
	case []byte:
		return value, true
	case string:
		return []byte(value), true
	default:
		return nil, false
	}
}

func getUUID(v interface{}) (uuid.UUID, bool) {
	switch value := v.(type) {
	case uuid.UUID:
		return value, true
	case string:
		parsed, err := uuid.Parse(value)
		if err == nil {
			return parsed, true
		}
	case [16]byte:
		parsed, err := uuid.FromBytes(value[:])
		if err == nil {
			return parsed, true
		}
	}

	return uuid.UUID{}, false
}

// getInt attempts to parse an integer from an interface
func getInt(v interface{}) (int64, bool) {
	switch value := v.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// getFloat attempts to parse a float from an interface
func getFloat(v interface{}) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// getRat attempts to parse a big.Rat from an interface
func getRat(v interface{}) (*big.Rat, bool) {
	switch value := v.(type) {
	case *big.Rat:
		return value, true
	case string:
		parsed, ok := new(big.Rat).SetString(value)
		if ok {
			return parsed, true
		}
	}
	return nil, false
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

// Equals checks if two QRecordBatches are identical.
func (q *QRecordBatch) Equals(other *QRecordBatch) bool {
	if other == nil {
		return q == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords || len(q.ColumnNames) != len(other.ColumnNames) {
		return false
	}

	// Compare column names
	for i, colName := range q.ColumnNames {
		if colName != other.ColumnNames[i] {
			return false
		}
	}

	// Compare records
	for i, record := range q.Records {
		if !record.equals(other.Records[i]) {
			return false
		}
	}

	return true
}
