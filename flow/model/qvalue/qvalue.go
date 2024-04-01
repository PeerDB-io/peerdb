package qvalue

import (
	"time"

	"github.com/shopspring/decimal"
)

// if new types are added, register them in gob - cdc_store.go
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
	return QValueKindFloat32
}

func (v QValueFloat32) Value() any {
	return v.Val
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

type QValueInt16 struct {
	Val int16
}

func (QValueInt16) Kind() QValueKind {
	return QValueKindInt16
}

func (v QValueInt16) Value() any {
	return v.Val
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

type QValueInt64 struct {
	Val int64
}

func (QValueInt64) Kind() QValueKind {
	return QValueKindInt64
}

func (v QValueInt64) Value() any {
	return v.Val
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

type QValueStruct struct {
	Val map[string]interface{}
}

func (QValueStruct) Kind() QValueKind {
	return QValueKindStruct
}

func (v QValueStruct) Value() any {
	return v.Val
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

type QValueString struct {
	Val string
}

func (QValueString) Kind() QValueKind {
	return QValueKindString
}

func (v QValueString) Value() any {
	return v.Val
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

type QValueTimestampTZ struct {
	Val time.Time
}

func (QValueTimestampTZ) Kind() QValueKind {
	return QValueKindTimestampTZ
}

func (v QValueTimestampTZ) Value() any {
	return v.Val
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

type QValueTime struct {
	Val time.Time
}

func (QValueTime) Kind() QValueKind {
	return QValueKindTime
}

func (v QValueTime) Value() any {
	return v.Val
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

type QValueInterval struct {
	Val string
}

func (QValueInterval) Kind() QValueKind {
	return QValueKindInterval
}

func (v QValueInterval) Value() any {
	return v.Val
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

type QValueBytes struct {
	Val []byte
}

func (QValueBytes) Kind() QValueKind {
	return QValueKindBytes
}

func (v QValueBytes) Value() any {
	return v.Val
}

type QValueUUID struct {
	Val [16]byte
}

func (QValueUUID) Kind() QValueKind {
	return QValueKindUUID
}

func (v QValueUUID) Value() any {
	return v.Val
}

type QValueJSON struct {
	Val string
}

func (QValueJSON) Kind() QValueKind {
	return QValueKindJSON
}

func (v QValueJSON) Value() any {
	return v.Val
}

type QValueBit struct {
	Val []byte
}

func (QValueBit) Kind() QValueKind {
	return QValueKindBit
}

func (v QValueBit) Value() any {
	return v.Val
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

type QValueGeography struct {
	Val string
}

func (QValueGeography) Kind() QValueKind {
	return QValueKindGeography
}

func (v QValueGeography) Value() any {
	return v.Val
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

type QValuePoint struct {
	Val string
}

func (QValuePoint) Kind() QValueKind {
	return QValueKindPoint
}

func (v QValuePoint) Value() any {
	return v.Val
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

type QValueINET struct {
	Val string
}

func (QValueINET) Kind() QValueKind {
	return QValueKindINET
}

func (v QValueINET) Value() any {
	return v.Val
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

type QValueArrayFloat32 struct {
	Val []float32
}

func (QValueArrayFloat32) Kind() QValueKind {
	return QValueKindArrayFloat32
}

func (v QValueArrayFloat32) Value() any {
	return v.Val
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

type QValueArrayInt16 struct {
	Val []int16
}

func (QValueArrayInt16) Kind() QValueKind {
	return QValueKindInt16
}

func (v QValueArrayInt16) Value() any {
	return v.Val
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

type QValueArrayInt64 struct {
	Val []int64
}

func (QValueArrayInt64) Kind() QValueKind {
	return QValueKindArrayInt64
}

func (v QValueArrayInt64) Value() any {
	return v.Val
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

type QValueArrayDate struct {
	Val []time.Time
}

func (QValueArrayDate) Kind() QValueKind {
	return QValueKindArrayDate
}

func (v QValueArrayDate) Value() any {
	return v.Val
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

type QValueArrayTimestampTZ struct {
	Val []time.Time
}

func (QValueArrayTimestampTZ) Kind() QValueKind {
	return QValueKindArrayTimestampTZ
}

func (v QValueArrayTimestampTZ) Value() any {
	return v.Val
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
