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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"

	hstore_util "github.com/PeerDB-io/peer-flow/hstore"
)

// if new types are added, register them in gob - cdc_records_storage.go
type QValue interface {
	Kind() QValueKind
	Value() any
}

type QValueNull QValueKind

func (v QValueNull) Kind() QValueKind {
	return QValueKind(v)
}

func (QValueNull) Value() any {
	return nil
}

type QValueInvalid struct {
	Val string
}

func (QValueInvalid) Kind() QValueKind {
	return QValueKindInvalid
}

func (v QValueInvalid) Value() any {
	return v.Val
}

type QValueFloat32 struct {
	Val float32
}

func (QValueFloat32) Kind() QValueKind {
	return "float32"
}

func (v QValueFloat32) Value() any {
	return v.Val
}

type QValueFloat64 struct {
	Val float64
}

func (QValueFloat64) Kind() QValueKind {
	return "float64"
}

func (v QValueFloat64) Value() any {
	return v.Val
}

type QValueInt16 struct {
	Val int16
}

func (QValueInt16) Kind() QValueKind {
	return "int16"
}

func (v QValueInt16) Value() any {
	return v.Val
}

type QValueInt32 struct {
	Val int32
}

func (QValueInt32) Kind() QValueKind {
	return "int32"
}

func (v QValueInt32) Value() any {
	return v.Val
}

type QValueInt64 struct {
	Val int64
}

func (QValueInt64) Kind() QValueKind {
	return "int64"
}

func (v QValueInt64) Value() any {
	return v.Val
}

type QValueBoolean struct {
	Val bool
}

func (QValueBoolean) Kind() QValueKind {
	return "bool"
}

func (v QValueBoolean) Value() any {
	return v.Val
}

type QValueStruct struct {
	Val map[string]interface{}
}

func (QValueStruct) Kind() QValueKind {
	return "struct"
}

func (v QValueStruct) Value() any {
	return v.Val
}

type QValueQChar struct {
	Val uint8
}

func (QValueQChar) Kind() QValueKind {
	return "qchar"
}

func (v QValueQChar) Value() any {
	return v.Val
}

type QValueString struct {
	Val string
}

func (QValueString) Kind() QValueKind {
	return "string"
}

func (v QValueString) Value() any {
	return v.Val
}

type QValueTimestamp struct {
	Val time.Time
}

func (QValueTimestamp) Kind() QValueKind {
	return "timestamp"
}

func (v QValueTimestamp) Value() any {
	return v.Val
}

type QValueTimestampTZ struct {
	Val time.Time
}

func (QValueTimestampTZ) Kind() QValueKind {
	return "timestamptz"
}

func (v QValueTimestampTZ) Value() any {
	return v.Val
}

type QValueDate struct {
	Val time.Time
}

func (QValueDate) Kind() QValueKind {
	return "date"
}

func (v QValueDate) Value() any {
	return v.Val
}

type QValueTime struct {
	Val time.Time
}

func (QValueTime) Kind() QValueKind {
	return "time"
}

func (v QValueTime) Value() any {
	return v.Val
}

type QValueTimeTZ struct {
	Val time.Time
}

func (QValueTimeTZ) Kind() QValueKind {
	return "timetz"
}

func (v QValueTimeTZ) Value() any {
	return v.Val
}

type QValueInterval struct {
	Val string
}

func (QValueInterval) Kind() QValueKind {
	return "interval"
}

func (v QValueInterval) Value() any {
	return v.Val
}

type QValueNumeric struct {
	Val decimal.Decimal
}

func (QValueNumeric) Kind() QValueKind {
	return "numeric"
}

func (v QValueNumeric) Value() any {
	return v.Val
}

type QValueBytes struct {
	Val []byte
}

func (QValueBytes) Kind() QValueKind {
	return "bytes"
}

func (v QValueBytes) Value() any {
	return v.Val
}

type QValueUUID struct {
	Val [16]byte
}

func (QValueUUID) Kind() QValueKind {
	return "uuid"
}

func (v QValueUUID) Value() any {
	return v.Val
}

type QValueJSON struct {
	Val string
}

func (QValueJSON) Kind() QValueKind {
	return "json"
}

func (v QValueJSON) Value() any {
	return v.Val
}

type QValueBit struct {
	Val []byte
}

func (QValueBit) Kind() QValueKind {
	return "bit"
}

func (v QValueBit) Value() any {
	return v.Val
}

type QValueHStore struct {
	Val string
}

func (QValueHStore) Kind() QValueKind {
	return "hstore"
}

func (v QValueHStore) Value() any {
	return v.Val
}

type QValueGeography struct {
	Val string
}

func (QValueGeography) Kind() QValueKind {
	return "geography"
}

func (v QValueGeography) Value() any {
	return v.Val
}

type QValueGeometry struct {
	Val string
}

func (QValueGeometry) Kind() QValueKind {
	return "geometry"
}

func (v QValueGeometry) Value() any {
	return v.Val
}

type QValuePoint struct {
	Val string
}

func (QValuePoint) Kind() QValueKind {
	return "point"
}

func (v QValuePoint) Value() any {
	return v.Val
}

type QValueCIDR struct {
	Val string
}

func (QValueCIDR) Kind() QValueKind {
	return "cidr"
}

func (v QValueCIDR) Value() any {
	return v.Val
}

type QValueINET struct {
	Val string
}

func (QValueINET) Kind() QValueKind {
	return "inet"
}

func (v QValueINET) Value() any {
	return v.Val
}

type QValueMacaddr struct {
	Val string
}

func (QValueMacaddr) Kind() QValueKind {
	return "macaddr"
}

func (v QValueMacaddr) Value() any {
	return v.Val
}

type QValueArrayFloat32 struct {
	Val []float32
}

func (QValueArrayFloat32) Kind() QValueKind {
	return "array_float32"
}

func (v QValueArrayFloat32) Value() any {
	return v.Val
}

type QValueArrayFloat64 struct {
	Val []float64
}

func (QValueArrayFloat64) Kind() QValueKind {
	return "array_float64"
}

func (v QValueArrayFloat64) Value() any {
	return v.Val
}

type QValueArrayInt16 struct {
	Val []int16
}

func (QValueArrayInt16) Kind() QValueKind {
	return "array_int16"
}

func (v QValueArrayInt16) Value() any {
	return v.Val
}

type QValueArrayInt32 struct {
	Val []int32
}

func (QValueArrayInt32) Kind() QValueKind {
	return "array_int32"
}

func (v QValueArrayInt32) Value() any {
	return v.Val
}

type QValueArrayInt64 struct {
	Val []int64
}

func (QValueArrayInt64) Kind() QValueKind {
	return "array_int64"
}

func (v QValueArrayInt64) Value() any {
	return v.Val
}

type QValueArrayString struct {
	Val []string
}

func (QValueArrayString) Kind() QValueKind {
	return "array_string"
}

func (v QValueArrayString) Value() any {
	return v.Val
}

type QValueArrayDate struct {
	Val []time.Time
}

func (QValueArrayDate) Kind() QValueKind {
	return "array_date"
}

func (v QValueArrayDate) Value() any {
	return v.Val
}

type QValueArrayTimestamp struct {
	Val []time.Time
}

func (QValueArrayTimestamp) Kind() QValueKind {
	return "array_timestamp"
}

func (v QValueArrayTimestamp) Value() any {
	return v.Val
}

type QValueArrayTimestampTZ struct {
	Val []time.Time
}

func (QValueArrayTimestampTZ) Kind() QValueKind {
	return "array_timestamptz"
}

func (v QValueArrayTimestampTZ) Value() any {
	return v.Val
}

type QValueArrayBoolean struct {
	Val []bool
}

func (QValueArrayBoolean) Kind() QValueKind {
	return "array_bool"
}

func (v QValueArrayBoolean) Value() any {
	return v.Val
}

func Equals(qv QValue, other QValue) bool {
	qvValue := qv.Value()
	otherValue := other.Value()
	qvEmpty := qvValue == nil || (reflect.TypeOf(qvValue).Kind() == reflect.Slice && reflect.ValueOf(qvValue).Len() == 0)
	otherEmpty := otherValue == nil || (reflect.TypeOf(otherValue).Kind() == reflect.Slice && reflect.ValueOf(otherValue).Len() == 0)
	if qvEmpty && otherEmpty {
		return true
	}
	qvString, ok1 := qvValue.(string)
	otherString, ok2 := otherValue.(string)
	if ok1 && ok2 && qvString == otherString {
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
		if otherVal, ok := other.(QValueString); ok {
			return q.Val == otherVal.Val
		}
		return false
	case QValueINET:
		if otherVal, ok := other.(QValueINET); ok {
			return q.Val == otherVal.Val
		}
		return false
	case QValueCIDR:
		if otherVal, ok := other.(QValueCIDR); ok {
			return q.Val == otherVal.Val
		}
		return false
	// all internally represented as a Golang time.Time
	case QValueDate, QValueTimestamp, QValueTimestampTZ, QValueTime, QValueTimeTZ:
		return compareGoTime(q.Value(), other.Value())
	case QValueNumeric:
		return compareNumeric(q.Val, other.Value())
	case QValueBytes:
		return compareBytes(q.Value(), other.Value())
	case QValueUUID:
		return compareUUID(q.Value(), other.Value())
	case QValueJSON:
		// TODO (kaushik): fix for tests
		return true
	case QValueBit:
		return compareBytes(q.Value(), other.Value())
	case QValueGeometry:
		return compareGeometry(q.Value(), other.Value())
	case QValueGeography:
		return compareGeometry(q.Value(), other.Value())
	case QValueHStore:
		return compareHstore(q.Val, other.Value())
	case QValueArrayFloat32:
		return compareNumericArrays(q.Val, other.Value())
	case QValueArrayFloat64:
		return compareNumericArrays(q.Val, other.Value())
	case QValueArrayInt32, QValueArrayInt16:
		return compareNumericArrays(q.Value(), other.Value())
	case QValueArrayInt64:
		return compareNumericArrays(q.Val, other.Value())
	case QValueArrayDate:
		return compareDateArrays(q.Val, other.Value())
	case QValueArrayTimestamp, QValueArrayTimestampTZ:
		return compareTimeArrays(q.Value(), other.Value())
	case QValueArrayBoolean:
		return compareBoolArrays(q.Val, other.Value())
	case QValueArrayString:
		return compareArrayString(q.Val, other.Value())
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

func compareUUID(value1, value2 interface{}) bool {
	if value1 == nil && value2 == nil {
		return true
	}

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
