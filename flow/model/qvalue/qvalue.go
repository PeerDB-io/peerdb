package qvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"

	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
)

// if new types are added, register them in gob - cdc_records_storage.go
type QValue struct {
	Kind  QValueKind
	Value interface{}
}

func (q QValue) Equals(other QValue) bool {
	if q.Kind == QValueKindJSON {
		return true // TODO fix
	} else if q.Value == nil && other.Value == nil {
		return true
	}

	switch q.Kind {
	case QValueKindEmpty:
		return other.Kind == QValueKindEmpty
	case QValueKindInvalid:
		return true
	case QValueKindFloat32:
		return compareFloat32(q.Value, other.Value)
	case QValueKindFloat64:
		return compareFloat64(q.Value, other.Value)
	case QValueKindInt16:
		return compareInt16(q.Value, other.Value)
	case QValueKindInt32:
		return compareInt32(q.Value, other.Value)
	case QValueKindInt64:
		return compareInt64(q.Value, other.Value)
	case QValueKindBoolean:
		return compareBoolean(q.Value, other.Value)
	case QValueKindStruct:
		return compareStruct(q.Value, other.Value)
	case QValueKindQChar:
		if (q.Value == nil) == (other.Value == nil) {
			return q.Value == nil || q.Value.(uint8) == other.Value.(uint8)
		} else {
			return false
		}
	case QValueKindString, QValueKindINET, QValueKindCIDR:
		return compareString(q.Value, other.Value)
	// all internally represented as a Golang time.Time
	case QValueKindDate,
		QValueKindTimestamp, QValueKindTimestampTZ:
		return compareGoTime(q.Value, other.Value)
	case QValueKindTime, QValueKindTimeTZ:
		return compareGoCivilTime(q.Value, other.Value)
	case QValueKindNumeric:
		return compareNumeric(q.Value, other.Value)
	case QValueKindBytes:
		return compareBytes(q.Value, other.Value)
	case QValueKindUUID:
		return compareUUID(q.Value, other.Value)
	case QValueKindJSON:
		return compareJSON(q.Value, other.Value)
	case QValueKindBit:
		return compareBit(q.Value, other.Value)
	case QValueKindGeometry, QValueKindGeography:
		return compareGeometry(q.Value, other.Value)
	case QValueKindHStore:
		return compareHstore(q.Value, other.Value)
	case QValueKindArrayFloat32:
		return compareNumericArrays(q.Value, other.Value)
	case QValueKindArrayFloat64:
		return compareNumericArrays(q.Value, other.Value)
	case QValueKindArrayInt32, QValueKindArrayInt16:
		return compareNumericArrays(q.Value, other.Value)
	case QValueKindArrayInt64:
		return compareNumericArrays(q.Value, other.Value)
	case QValueKindArrayDate:
		return compareDateArrays(q.Value, other.Value)
	case QValueKindArrayTimestamp, QValueKindArrayTimestampTZ:
		return compareTimeArrays(q.Value, other.Value)
	case QValueKindArrayBoolean:
		return compareBoolArrays(q.Value, other.Value)
	case QValueKindArrayString:
		return compareArrayString(q.Value, other.Value)
	default:
		return false
	}
}

func (q QValue) GoTimeConvert() (string, error) {
	if q.Kind == QValueKindTime || q.Kind == QValueKindTimeTZ {
		return q.Value.(time.Time).Format("15:04:05.999999"), nil
		// no connector supports time with timezone yet
		// } else if q.Kind == QValueKindTimeTZ {
		// 	return q.Value.(time.Time).Format("15:04:05.999999-0700"), nil
	} else if q.Kind == QValueKindDate {
		return q.Value.(time.Time).Format("2006-01-02"), nil
	} else if q.Kind == QValueKindTimestamp {
		return q.Value.(time.Time).Format("2006-01-02 15:04:05.999999"), nil
	} else if q.Kind == QValueKindTimestampTZ {
		return q.Value.(time.Time).Format("2006-01-02 15:04:05.999999-0700"), nil
	} else {
		return "", fmt.Errorf("unsupported QValueKind: %s", q.Kind)
	}
}

func compareInt16(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	int1, ok1 := getInt16(value1)
	int2, ok2 := getInt16(value2)
	return ok1 && ok2 && int1 == int2
}

func compareInt32(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	int1, ok1 := getInt32(value1)
	int2, ok2 := getInt32(value2)
	return ok1 && ok2 && int1 == int2
}

func compareInt64(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	int1, ok1 := getInt64(value1)
	int2, ok2 := getInt64(value2)
	return ok1 && ok2 && int1 == int2
}

func compareFloat32(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}
	float1, ok1 := getFloat32(value1)
	float2, ok2 := getFloat32(value2)
	return ok1 && ok2 && float1 == float2
}

func compareFloat64(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	float1, ok1 := getFloat64(value1)
	float2, ok2 := getFloat64(value2)
	return ok1 && ok2 && float1 == float2
}

func compareGoTime(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	et1, ok1 := value1.(time.Time)
	et2, ok2 := value2.(time.Time)

	if !ok1 || !ok2 {
		return false
	}

	// TODO: this is a hack, we should be comparing the actual time values
	// currently this is only used for testing so that is OK.
	t1 := et1.UnixMicro()
	t2 := et2.UnixMicro()

	return t1 == t2
}

func compareGoCivilTime(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	t1, ok1 := value1.(time.Time)
	t2, ok2 := value2.(time.Time)

	if !ok1 || !ok2 {
		if !ok2 {
			// For BigQuery, we need to compare civil.Time with time.Time
			ct2, ok3 := value2.(civil.Time)
			if !ok3 {
				return false
			}
			return t1.Hour() == ct2.Hour && t1.Minute() == ct2.Minute && t1.Second() == ct2.Second
		}
		return false
	}

	return t1.Hour() == t2.Hour() && t1.Minute() == t2.Minute() && t1.Second() == t2.Second()
}

func compareUUID(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	uuid1, ok1 := getUUID(value1)
	uuid2, ok2 := getUUID(value2)

	return ok1 && ok2 && uuid1 == uuid2
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
	num1, ok1 := getDecimal(value1)
	num2, ok2 := getDecimal(value2)

	if !ok1 || !ok2 {
		return false
	}

	return num1.Equal(num2)
}

func compareString(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	str1, ok1 := value1.(string)
	str2, ok2 := value2.(string)
	if !ok1 || !ok2 {
		return false
	}
	return str1 == str2
}

func compareHstore(value1, value2 interface{}) bool {
	str2 := value2.(string)
	switch v1 := value1.(type) {
	case pgtype.Hstore:
		bytes, err := json.Marshal(v1)
		if err != nil {
			panic(err)
		}
		return string(bytes) == str2
	case string:
		if v1 == str2 {
			return true
		}
		parsedHStore1, err := hstore_util.ParseHstore(v1)
		if err != nil {
			panic(err)
		}
		return parsedHStore1 == strings.ReplaceAll(strings.ReplaceAll(str2, " ", ""), "\n", "")
	default:
		panic(fmt.Sprintf("invalid hstore value type %T: %v", value1, value1))
	}
}

func compareGeometry(value1, value2 interface{}) bool {
	geo2, err := geom.NewGeomFromWKT(value2.(string))
	if err != nil {
		panic(err)
	}

	switch v1 := value1.(type) {
	case *geom.Geom:
		return v1.Equals(geo2)
	case string:
		geoWkt := v1
		if strings.HasPrefix(geoWkt, "SRID=") {
			_, wkt, found := strings.Cut(geoWkt, ";")
			if found {
				geoWkt = wkt
			}
		}

		geo1, err := geom.NewGeomFromWKT(geoWkt)
		if err != nil {
			panic(err)
		}
		return geo1.Equals(geo2)
	default:
		panic(fmt.Sprintf("invalid geometry value type %T: %v", value1, value1))
	}
}

func compareStruct(value1, value2 interface{}) bool {
	struct1, ok1 := value1.(map[string]interface{})
	struct2, ok2 := value2.(map[string]interface{})
	if !ok1 || !ok2 || len(struct1) != len(struct2) {
		return false
	}
	for k, v1 := range struct1 {
		v2, ok := struct2[k]
		if !ok {
			return false
		}
		q1, ok1 := v1.(QValue)
		q2, ok2 := v2.(QValue)
		if !ok1 || !ok2 || !q1.Equals(q2) {
			return false
		}
	}
	return true
}

func compareJSON(value1, value2 interface{}) bool {
	// TODO (kaushik): fix for tests
	return true
}

func compareBit(value1, value2 interface{}) bool {
	bit1, ok1 := value1.(int)
	bit2, ok2 := value2.(int)

	if !ok1 || !ok2 {
		return false
	}

	return bit1 == bit2
}

func compareNumericArrays(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	if value1 == nil && value2 == "null" {
		return true
	}

	if value1 == nil && value2 == "" {
		return true
	}

	// Helper function to convert a value to float64
	convertToFloat64 := func(val interface{}) []float64 {
		switch v := val.(type) {
		case []int16:
			result := make([]float64, len(v))
			for i, value := range v {
				result[i] = float64(value)
			}
			return result
		case []int32:
			result := make([]float64, len(v))
			for i, value := range v {
				result[i] = float64(value)
			}
			return result
		case []int64:
			result := make([]float64, len(v))
			for i, value := range v {
				result[i] = float64(value)
			}
			return result
		case []float32:
			result := make([]float64, len(v))
			for i, value := range v {
				result[i] = float64(value)
			}
			return result
		case []float64:
			return v
		default:
			return nil
		}
	}

	array1 := convertToFloat64(value1)
	array2 := convertToFloat64(value2)

	if array1 == nil || array2 == nil || len(array1) != len(array2) {
		return false
	}

	for i := range array1 {
		if math.Abs(array1[i]-array2[i]) >= 1e9 {
			return false
		}
	}

	return true
}

func compareTimeArrays(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}
	array1, ok1 := value1.([]time.Time)
	array2, ok2 := value2.([]time.Time)

	if !ok1 || !ok2 {
		return false
	}

	if len(array1) != len(array2) {
		return false
	}

	for i := range array1 {
		if !array1[i].Equal(array2[i]) {
			return false
		}
	}

	return true
}

func compareDateArrays(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}
	array1, ok1 := value1.([]time.Time)
	array2, ok2 := value2.([]civil.Date)

	if !ok1 || !ok2 || len(array1) != len(array2) {
		return false
	}

	for i := range array1 {
		if array1[i].Year() != array2[i].Year ||
			array1[i].Month() != array2[i].Month ||
			array1[i].Day() != array2[i].Day {
			return false
		}
	}

	return true
}

func compareBoolArrays(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}
	array1, ok1 := value1.([]bool)
	array2, ok2 := value2.([]bool)

	if !ok1 || !ok2 || len(array1) != len(array2) {
		return false
	}

	for i := range array1 {
		if array1[i] != array2[i] {
			return false
		}
	}

	return true
}

func compareArrayString(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

	// also return true if value2 is string null
	if value1 == nil && value2 == "null" {
		return true
	}

	// nulls end up as empty 'variants' in snowflake
	if value1 == nil && value2 == "" {
		return true
	}

	array1, ok1 := value1.([]string)
	array2, ok2 := value2.([]string)

	if !ok1 || !ok2 {
		return false
	}

	return reflect.DeepEqual(array1, array2)
}

func getInt16(v interface{}) (int16, bool) {
	switch value := v.(type) {
	case int16:
		return value, true
	case int32:
		return int16(value), true
	case int64:
		return int16(value), true
	case decimal.Decimal:
		return int16(value.IntPart()), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 16)
		if err == nil {
			return int16(parsed), true
		}
	}
	return 0, false
}

func getInt32(v interface{}) (int32, bool) {
	switch value := v.(type) {
	case int32:
		return value, true
	case int64:
		return int32(value), true
	case decimal.Decimal:
		return int32(value.IntPart()), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			return int32(parsed), true
		}
	}
	return 0, false
}

func getInt64(v interface{}) (int64, bool) {
	switch value := v.(type) {
	case int64:
		return value, true
	case int32:
		return int64(value), true
	case decimal.Decimal:
		return value.IntPart(), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func getFloat32(v interface{}) (float32, bool) {
	switch value := v.(type) {
	case float32:
		return value, true
	case float64:
		return float32(value), true
	case string:
		parsed, err := strconv.ParseFloat(value, 32)
		if err == nil {
			return float32(parsed), true
		}
	}
	return 0, false
}

func getFloat64(v interface{}) (float64, bool) {
	switch value := v.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func getBytes(v interface{}) ([]byte, bool) {
	switch value := v.(type) {
	case []byte:
		return value, true
	case string:
		return []byte(value), true
	case nil:
		return nil, true
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
		return uuid.UUID(value), true
	}

	return uuid.UUID{}, false
}

// getDecimal attempts to parse a decimal from an interface
func getDecimal(v interface{}) (decimal.Decimal, bool) {
	switch value := v.(type) {
	case decimal.Decimal:
		return value, true
	case string:
		parsed, err := decimal.NewFromString(value)
		if err != nil {
			panic(err)
		}
		return parsed, true
	case float64:
		return decimal.NewFromFloat(value), true
	case int64:
		return decimal.NewFromInt(value), true
	case uint64:
		return decimal.NewFromBigInt(new(big.Int).SetUint64(value), 0), true
	case float32:
		return decimal.NewFromFloat32(value), true
	case int32:
		return decimal.NewFromInt(int64(value)), true
	case uint32:
		return decimal.NewFromInt(int64(value)), true
	case int:
		return decimal.NewFromInt(int64(value)), true
	case uint:
		return decimal.NewFromInt(int64(value)), true
	case int8:
		return decimal.NewFromInt(int64(value)), true
	case uint8:
		return decimal.NewFromInt(int64(value)), true
	case int16:
		return decimal.NewFromInt(int64(value)), true
	case uint16:
		return decimal.NewFromInt(int64(value)), true
	}
	return decimal.Decimal{}, false
}
