package qvalue

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strconv"

	"github.com/google/uuid"
)

type QValue struct {
	Kind  QValueKind
	Value interface{}
}

func (q *QValue) Equals(other *QValue) bool {
	switch q.Kind {
	case QValueKindInvalid:
		return false // both are invalid we always return false
	case QValueKindFloat16:
		return compareFloat32(q.Value, other.Value)
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
	case QValueKindArray:
		return compareArray(q.Value, other.Value)
	case QValueKindStruct:
		return compareStruct(q.Value, other.Value)
	case QValueKindString:
		return compareString(q.Value, other.Value)
	case QValueKindETime:
		return compareETime(q.Value, other.Value)
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
	}

	return false
}

func compareInt16(value1, value2 interface{}) bool {
	int1, ok1 := getInt16(value1)
	int2, ok2 := getInt16(value2)
	return ok1 && ok2 && int1 == int2
}

func compareInt32(value1, value2 interface{}) bool {
	int1, ok1 := getInt32(value1)
	int2, ok2 := getInt32(value2)
	return ok1 && ok2 && int1 == int2
}

func compareInt64(value1, value2 interface{}) bool {
	int1, ok1 := getInt64(value1)
	int2, ok2 := getInt64(value2)
	return ok1 && ok2 && int1 == int2
}

func compareFloat32(value1, value2 interface{}) bool {
	float1, ok1 := getFloat32(value1)
	float2, ok2 := getFloat32(value2)
	return ok1 && ok2 && float1 == float2
}

func compareFloat64(value1, value2 interface{}) bool {
	float1, ok1 := getFloat64(value1)
	float2, ok2 := getFloat64(value2)
	return ok1 && ok2 && float1 == float2
}

func compareETime(value1, value2 interface{}) bool {
	et1, ok1 := value1.(*ExtendedTime)
	et2, ok2 := value2.(*ExtendedTime)

	if !ok1 || !ok2 {
		return false
	}

	// TODO: this is a hack, we should be comparing the actual time values
	// currently this is only used for testing so that is OK.
	t1 := et1.Time.UnixMilli() / 1000
	t2 := et2.Time.UnixMilli() / 1000

	return t1 == t2
}

func compareUUID(value1, value2 interface{}) bool {
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
	rat1, ok1 := getRat(value1)
	rat2, ok2 := getRat(value2)

	if !ok1 || !ok2 {
		return false
	}

	// check if the difference is less than 1e-9
	diff := new(big.Rat).Sub(rat1, rat2)
	return diff.Abs(diff).Cmp(big.NewRat(1, 1000000000)) < 0
}

func compareString(value1, value2 interface{}) bool {
	str1, ok1 := value1.(string)
	str2, ok2 := value2.(string)

	return ok1 && ok2 && str1 == str2
}

func compareArray(value1, value2 interface{}) bool {
	array1, ok1 := value1.([]interface{})
	array2, ok2 := value2.([]interface{})
	if !ok1 || !ok2 || len(array1) != len(array2) {
		return false
	}
	for i := range array1 {
		q1, ok1 := array1[i].(*QValue)
		q2, ok2 := array2[i].(*QValue)
		if !ok1 || !ok2 || !q1.Equals(q2) {
			return false
		}
	}
	return true
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
		q1, ok1 := v1.(*QValue)
		q2, ok2 := v2.(*QValue)
		if !ok1 || !ok2 || !q1.Equals(q2) {
			return false
		}
	}
	return true
}

func compareJSON(value1, value2 interface{}) bool {
	json1, ok1 := value1.(json.RawMessage)
	json2, ok2 := value2.(json.RawMessage)

	if !ok1 || !ok2 {
		return false
	}

	// Unmarshal to empty interfaces and then compare
	var obj1, obj2 interface{}
	err1 := json.Unmarshal(json1, &obj1)
	err2 := json.Unmarshal(json2, &obj2)

	if err1 != nil || err2 != nil {
		return false
	}

	return reflect.DeepEqual(obj1, obj2)
}

func compareBit(value1, value2 interface{}) bool {
	bit1, ok1 := value1.(int)
	bit2, ok2 := value2.(int)

	if !ok1 || !ok2 {
		return false
	}

	return bit1^bit2 == 0
}

func getInt16(v interface{}) (int16, bool) {
	switch value := v.(type) {
	case int16:
		return value, true
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
	case *big.Rat:
		return int32(value.Num().Int64()), true
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
	case *big.Rat:
		return value.Num().Int64(), true
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

// getRat attempts to parse a big.Rat from an interface
func getRat(v interface{}) (*big.Rat, bool) {
	switch value := v.(type) {
	case *big.Rat:
		return value, true
	case string:
		//nolint:gosec
		parsed, ok := new(big.Rat).SetString(value)
		if ok {
			return parsed, true
		}
	}
	return nil, false
}
