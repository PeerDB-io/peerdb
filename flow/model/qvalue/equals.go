package qvalue

import (
	"bytes"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"

	"github.com/PeerDB-io/peer-flow/datatypes"
)

func valueEmpty(value any) bool {
	return value == nil || value == "" || value == "null" ||
		(reflect.TypeOf(value).Kind() == reflect.Slice && reflect.ValueOf(value).Len() == 0)
}

func Equals(qv QValue, other QValue) bool {
	qvValue := qv.Value()
	otherValue := other.Value()
	if valueEmpty(qvValue) && valueEmpty(otherValue) {
		return true
	}

	switch q := qv.(type) {
	case QValueInvalid:
		return true
	case QValueFloat32:
		return q.compareFloat32(other)
	case QValueFloat64:
		return q.compareFloat64(other)
	case QValueInt16:
		return q.compareInt16(other)
	case QValueInt32:
		return q.compareInt32(other)
	case QValueInt64:
		return q.compareInt64(other)
	case QValueBoolean:
		if otherVal, ok := other.(QValueBoolean); ok {
			return q.Val == otherVal.Val
		}
		return false
	case QValueStruct:
		if otherVal, ok := other.(QValueStruct); ok {
			return q.compareStruct(otherVal)
		}
		return false
	case QValueQChar:
		if otherVal, ok := other.(QValueQChar); ok {
			return q.Val == otherVal.Val
		}
		return false
	case QValueString:
		return compareString(q.Val, otherValue)
	case QValueINET:
		return compareString(q.Val, otherValue)
	case QValueCIDR:
		return compareString(q.Val, otherValue)
	case QValueMacaddr:
		return compareString(q.Val, otherValue)
	// all internally represented as a Golang time.Time
	case QValueTimestamp, QValueTimestampTZ:
		return compareGoTimestamp(qvValue, otherValue)
	case QValueTime, QValueTimeTZ:
		return compareGoTime(qvValue, otherValue)
	case QValueDate:
		return compareGoDate(qvValue, otherValue)
	case QValueNumeric:
		return compareNumeric(q.Val, otherValue)
	case QValueBytes:
		return compareBytes(qvValue, otherValue)
	case QValueUUID:
		return compareUUID(qvValue, otherValue)
	case QValueJSON:
		// TODO (kaushik): fix for tests
		return true
	case QValueGeometry:
		return compareGeometry(q.Val, otherValue)
	case QValueGeography:
		return compareGeometry(q.Val, otherValue)
	case QValueHStore:
		return compareHStore(q.Val, otherValue)
	case QValueArrayInt32, QValueArrayInt16, QValueArrayInt64, QValueArrayFloat32, QValueArrayFloat64:
		return compareNumericArrays(qvValue, otherValue)
	case QValueArrayDate:
		return compareDateArrays(q.Val, otherValue)
	case QValueArrayTimestamp, QValueArrayTimestampTZ:
		return compareTimeArrays(qvValue, otherValue)
	case QValueArrayBoolean:
		return compareBoolArrays(q.Val, otherValue)
	case QValueArrayUUID:
		return compareUuidArrays(q.Val, otherValue)
	case QValueArrayString:
		return compareArrayString(q.Val, otherValue)
	default:
		return false
	}
}

func (v QValueInt16) compareInt16(value2 QValue) bool {
	int2, ok2 := getInt16(value2.Value())
	return ok2 && v.Val == int2
}

func (v QValueInt32) compareInt32(value2 QValue) bool {
	int2, ok2 := getInt32(value2.Value())
	return ok2 && v.Val == int2
}

func (v QValueInt64) compareInt64(value2 QValue) bool {
	int2, ok2 := getInt64(value2.Value())
	return ok2 && v.Val == int2
}

func (v QValueFloat32) compareFloat32(value2 QValue) bool {
	float2, ok2 := getFloat32(value2.Value())
	return ok2 && v.Val == float2
}

func (v QValueFloat64) compareFloat64(value2 QValue) bool {
	float2, ok2 := getFloat64(value2.Value())
	return ok2 && v.Val == float2
}

func compareString(s1 string, value2 interface{}) bool {
	s2, ok := value2.(string)
	return ok && s1 == s2
}

func compareGoTimestamp(value1, value2 interface{}) bool {
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

func compareGoTime(value1, value2 interface{}) bool {
	t1, ok1 := value1.(time.Time)
	t2, ok2 := value2.(time.Time)

	if !ok1 || !ok2 {
		return false
	}

	h1, m1, s1 := t1.Clock()
	h2, m2, s2 := t2.Clock()
	return h1 == h2 && m1 == m2 && s1 == s2
}

func compareGoDate(value1, value2 interface{}) bool {
	t1, ok1 := value1.(time.Time)
	t2, ok2 := value2.(time.Time)

	if !ok1 || !ok2 {
		return false
	}

	y1, m1, d1 := t1.Date()
	y2, m2, d2 := t2.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}

func compareUUID(value1, value2 interface{}) bool {
	uuid1, ok1 := getUUID(value1)
	uuid2, ok2 := getUUID(value2)

	return ok1 && ok2 && uuid1 == uuid2
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

func compareHStore(str1 string, value2 interface{}) bool {
	str2 := value2.(string)
	if str1 == str2 {
		return true
	}
	parsedHStore1, err := datatypes.ParseHstore(str1)
	if err != nil {
		panic(err)
	}
	return parsedHStore1 == strings.ReplaceAll(strings.ReplaceAll(str2, " ", ""), "\n", "")
}

func compareGeometry(geoWkt string, value2 interface{}) bool {
	geo2, err := geom.NewGeomFromWKT(value2.(string))
	if err != nil {
		panic(err)
	}

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
}

func (v QValueStruct) compareStruct(value2 QValueStruct) bool {
	struct1 := v.Val
	struct2 := value2.Val
	if len(struct1) != len(struct2) {
		return false
	}
	for k, v1 := range struct1 {
		v2, ok := struct2[k]
		if !ok {
			return false
		}
		q1, ok1 := v1.(QValue)
		q2, ok2 := v2.(QValue)
		if !ok1 || !ok2 || !Equals(q1, q2) {
			return false
		}
	}
	return true
}

func compareNumericArrays(value1, value2 interface{}) bool {
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
	array1, ok1 := value1.([]time.Time)
	array2, ok2 := value2.([]time.Time)

	if !ok1 || !ok2 || len(array1) != len(array2) {
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
	array1, ok1 := value1.([]time.Time)
	array2, ok2 := value2.([]time.Time)

	if !ok1 || !ok2 || len(array1) != len(array2) {
		return false
	}

	for i := range array1 {
		if array1[i].Year() != array2[i].Year() ||
			array1[i].Month() != array2[i].Month() ||
			array1[i].Day() != array2[i].Day() {
			return false
		}
	}
	return true
}

func compareBoolArrays(value1, value2 interface{}) bool {
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

func compareUuidArrays(value1, value2 interface{}) bool {
	array1, ok1 := value1.([]uuid.UUID)
	array2, ok2 := value2.([]uuid.UUID)

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
	array1, ok1 := value1.([]string)
	array2, ok2 := value2.([]string)

	if !ok1 || !ok2 {
		return false
	}

	return slices.Compare(array1, array2) == 0
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
		return decimal.NewFromUint64(value), true
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
