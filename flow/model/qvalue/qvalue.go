package qvalue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/glua64"
	"github.com/PeerDB-io/peer-flow/shared"
)

// if new types are added, register them in gob - cdc_store.go
type QValue interface {
	Kind() QValueKind
	Value() any
	LValue(ls *lua.LState) lua.LValue
}

type QValueNull QValueKind

func (v QValueNull) Kind() QValueKind {
	return QValueKind(v)
}

func (QValueNull) Value() any {
	return nil
}

func (QValueNull) LValue(ls *lua.LState) lua.LValue {
	return lua.LNil
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

func (v QValueInvalid) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueFloat32 struct {
	Val float32
}

func (QValueFloat32) Kind() QValueKind {
	return QValueKindFloat32
}

func (v QValueFloat32) Value() any {
	return v.Val
}

func (v QValueFloat32) LValue(ls *lua.LState) lua.LValue {
	return lua.LNumber(v.Val)
}

type QValueFloat64 struct {
	Val float64
}

func (QValueFloat64) Kind() QValueKind {
	return QValueKindFloat64
}

func (v QValueFloat64) Value() any {
	return v.Val
}

func (v QValueFloat64) LValue(ls *lua.LState) lua.LValue {
	return lua.LNumber(v.Val)
}

type QValueInt16 struct {
	Val int16
}

func (QValueInt16) Kind() QValueKind {
	return QValueKindInt16
}

func (v QValueInt16) Value() any {
	return v.Val
}

func (v QValueInt16) LValue(ls *lua.LState) lua.LValue {
	return lua.LNumber(v.Val)
}

type QValueInt32 struct {
	Val int32
}

func (QValueInt32) Kind() QValueKind {
	return QValueKindInt32
}

func (v QValueInt32) Value() any {
	return v.Val
}

func (v QValueInt32) LValue(ls *lua.LState) lua.LValue {
	return lua.LNumber(v.Val)
}

type QValueInt64 struct {
	Val int64
}

func (QValueInt64) Kind() QValueKind {
	return QValueKindInt64
}

func (v QValueInt64) Value() any {
	return v.Val
}

func (v QValueInt64) LValue(ls *lua.LState) lua.LValue {
	return glua64.I64.New(ls, v.Val)
}

type QValueBoolean struct {
	Val bool
}

func (QValueBoolean) Kind() QValueKind {
	return QValueKindBoolean
}

func (v QValueBoolean) Value() any {
	return v.Val
}

func (v QValueBoolean) LValue(ls *lua.LState) lua.LValue {
	return lua.LBool(v.Val)
}

type QValueStruct struct {
	Val map[string]interface{}
}

func (QValueStruct) Kind() QValueKind {
	return QValueKindStruct
}

func (v QValueStruct) Value() any {
	return v.Val
}

func (v QValueStruct) LValue(ls *lua.LState) lua.LValue {
	bytes, err := json.Marshal(v.Val)
	if err != nil {
		return lua.LString(err.Error())
	} else {
		return lua.LString(shared.UnsafeFastReadOnlyBytesToString(bytes))
	}
}

type QValueQChar struct {
	Val uint8
}

func (QValueQChar) Kind() QValueKind {
	return QValueKindQChar
}

func (v QValueQChar) Value() any {
	return v.Val
}

func (v QValueQChar) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueString struct {
	Val string
}

func (QValueString) Kind() QValueKind {
	return QValueKindString
}

func (v QValueString) Value() any {
	return v.Val
}

func (v QValueString) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueTimestamp struct {
	Val time.Time
}

func (QValueTimestamp) Kind() QValueKind {
	return QValueKindTimestamp
}

func (v QValueTimestamp) Value() any {
	return v.Val
}

func (v QValueTimestamp) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaTime.New(ls, v.Val)
}

type QValueTimestampTZ struct {
	Val time.Time
}

func (QValueTimestampTZ) Kind() QValueKind {
	return QValueKindTimestampTZ
}

func (v QValueTimestampTZ) Value() any {
	return v.Val
}

func (v QValueTimestampTZ) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaTime.New(ls, v.Val)
}

type QValueDate struct {
	Val time.Time
}

func (QValueDate) Kind() QValueKind {
	return QValueKindDate
}

func (v QValueDate) Value() any {
	return v.Val
}

func (v QValueDate) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaTime.New(ls, v.Val)
}

type QValueTime struct {
	Val time.Time
}

func (QValueTime) Kind() QValueKind {
	return QValueKindTime
}

func (v QValueTime) Value() any {
	return v.Val
}

func (v QValueTime) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaTime.New(ls, v.Val)
}

type QValueTimeTZ struct {
	Val time.Time
}

func (QValueTimeTZ) Kind() QValueKind {
	return QValueKindTimeTZ
}

func (v QValueTimeTZ) Value() any {
	return v.Val
}

func (v QValueTimeTZ) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaTime.New(ls, v.Val)
}

type QValueInterval struct {
	Val string
}

func (QValueInterval) Kind() QValueKind {
	return QValueKindInterval
}

func (v QValueInterval) Value() any {
	return v.Val
}

func (v QValueInterval) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueTSTZRange struct {
	Val string
}

func (QValueTSTZRange) Kind() QValueKind {
	return QValueKindInterval
}

func (v QValueTSTZRange) Value() any {
	return v.Val
}

func (v QValueTSTZRange) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueNumeric struct {
	Val decimal.Decimal
}

func (QValueNumeric) Kind() QValueKind {
	return QValueKindNumeric
}

func (v QValueNumeric) Value() any {
	return v.Val
}

func (v QValueNumeric) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaDecimal.New(ls, v.Val)
}

type QValueBytes struct {
	Val []byte
}

func (QValueBytes) Kind() QValueKind {
	return QValueKindBytes
}

func (v QValueBytes) Value() any {
	return v.Val
}

func (v QValueBytes) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(shared.UnsafeFastReadOnlyBytesToString(v.Val))
}

type QValueUUID struct {
	Val uuid.UUID
}

func (QValueUUID) Kind() QValueKind {
	return QValueKindUUID
}

func (v QValueUUID) Value() any {
	return v.Val
}

func (v QValueUUID) LValue(ls *lua.LState) lua.LValue {
	return shared.LuaUuid.New(ls, v.Val)
}

type QValueArrayUUID struct {
	Val []uuid.UUID
}

func (QValueArrayUUID) Kind() QValueKind {
	return QValueKindArrayUUID
}

func (v QValueArrayUUID) Value() any {
	return v.Val
}

func (v QValueArrayUUID) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x uuid.UUID) lua.LValue {
		return shared.LuaUuid.New(ls, x)
	})
}

type QValueJSON struct {
	Val     string
	IsArray bool
}

func (QValueJSON) Kind() QValueKind {
	return QValueKindJSON
}

func (v QValueJSON) Value() any {
	return v.Val
}

func (v QValueJSON) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueHStore struct {
	Val string
}

func (QValueHStore) Kind() QValueKind {
	return QValueKindHStore
}

func (v QValueHStore) Value() any {
	return v.Val
}

func (v QValueHStore) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueGeography struct {
	Val string
}

func (QValueGeography) Kind() QValueKind {
	return QValueKindGeography
}

func (v QValueGeography) Value() any {
	return v.Val
}

func (v QValueGeography) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueGeometry struct {
	Val string
}

func (QValueGeometry) Kind() QValueKind {
	return QValueKindGeometry
}

func (v QValueGeometry) Value() any {
	return v.Val
}

func (v QValueGeometry) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValuePoint struct {
	Val string
}

func (QValuePoint) Kind() QValueKind {
	return QValueKindPoint
}

func (v QValuePoint) Value() any {
	return v.Val
}

func (v QValuePoint) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueCIDR struct {
	Val string
}

func (QValueCIDR) Kind() QValueKind {
	return QValueKindCIDR
}

func (v QValueCIDR) Value() any {
	return v.Val
}

func (v QValueCIDR) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueINET struct {
	Val string
}

func (QValueINET) Kind() QValueKind {
	return QValueKindINET
}

func (v QValueINET) Value() any {
	return v.Val
}

func (v QValueINET) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueMacaddr struct {
	Val string
}

func (QValueMacaddr) Kind() QValueKind {
	return QValueKindMacaddr
}

func (v QValueMacaddr) Value() any {
	return v.Val
}

func (v QValueMacaddr) LValue(ls *lua.LState) lua.LValue {
	return lua.LString(v.Val)
}

type QValueArrayFloat32 struct {
	Val []float32
}

func (QValueArrayFloat32) Kind() QValueKind {
	return QValueKindArrayFloat32
}

func (v QValueArrayFloat32) Value() any {
	return v.Val
}

func (v QValueArrayFloat32) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(f float32) lua.LValue {
		return lua.LNumber(f)
	})
}

type QValueArrayFloat64 struct {
	Val []float64
}

func (QValueArrayFloat64) Kind() QValueKind {
	return QValueKindArrayFloat64
}

func (v QValueArrayFloat64) Value() any {
	return v.Val
}

func (v QValueArrayFloat64) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x float64) lua.LValue {
		return lua.LNumber(x)
	})
}

type QValueArrayInt16 struct {
	Val []int16
}

func (QValueArrayInt16) Kind() QValueKind {
	return QValueKindInt16
}

func (v QValueArrayInt16) Value() any {
	return v.Val
}

func (v QValueArrayInt16) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x int16) lua.LValue {
		return lua.LNumber(x)
	})
}

type QValueArrayInt32 struct {
	Val []int32
}

func (QValueArrayInt32) Kind() QValueKind {
	return QValueKindInt32
}

func (v QValueArrayInt32) Value() any {
	return v.Val
}

func (v QValueArrayInt32) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x int32) lua.LValue {
		return lua.LNumber(x)
	})
}

type QValueArrayInt64 struct {
	Val []int64
}

func (QValueArrayInt64) Kind() QValueKind {
	return QValueKindArrayInt64
}

func (v QValueArrayInt64) Value() any {
	return v.Val
}

func (v QValueArrayInt64) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x int64) lua.LValue {
		return glua64.I64.New(ls, x)
	})
}

type QValueArrayString struct {
	Val []string
}

func (QValueArrayString) Kind() QValueKind {
	return QValueKindArrayString
}

func (v QValueArrayString) Value() any {
	return v.Val
}

func (v QValueArrayString) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x string) lua.LValue {
		return lua.LString(x)
	})
}

type QValueArrayDate struct {
	Val []time.Time
}

func (QValueArrayDate) Kind() QValueKind {
	return QValueKindArrayDate
}

func (v QValueArrayDate) Value() any {
	return v.Val
}

func (v QValueArrayDate) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x time.Time) lua.LValue {
		return shared.LuaTime.New(ls, x)
	})
}

type QValueArrayTimestamp struct {
	Val []time.Time
}

func (QValueArrayTimestamp) Kind() QValueKind {
	return QValueKindArrayTimestamp
}

func (v QValueArrayTimestamp) Value() any {
	return v.Val
}

func (v QValueArrayTimestamp) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x time.Time) lua.LValue {
		return shared.LuaTime.New(ls, x)
	})
}

type QValueArrayTimestampTZ struct {
	Val []time.Time
}

func (QValueArrayTimestampTZ) Kind() QValueKind {
	return QValueKindArrayTimestampTZ
}

func (v QValueArrayTimestampTZ) Value() any {
	return v.Val
}

func (v QValueArrayTimestampTZ) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x time.Time) lua.LValue {
		return shared.LuaTime.New(ls, x)
	})
}

type QValueArrayBoolean struct {
	Val []bool
}

func (QValueArrayBoolean) Kind() QValueKind {
	return QValueKindArrayBoolean
}

func (v QValueArrayBoolean) Value() any {
	return v.Val
}

func (v QValueArrayBoolean) LValue(ls *lua.LState) lua.LValue {
	return shared.SliceToLTable(ls, v.Val, func(x bool) lua.LValue {
		return lua.LBool(x)
	})
}
