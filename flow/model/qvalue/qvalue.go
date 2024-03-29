package qvalue

import (
	"time"

	"github.com/shopspring/decimal"
)

// if new types are added, register them in gob - cdc_records_storage.go
type QValue interface {
	Type() QType
	Value() any
}

type QValueNull QType

func (v QValueNull) Type() QType {
	return QType(v)
}

func (QValueNull) Value() any {
	return nil
}

type QValueInvalid struct {
	Val string
}

func (QValueInvalid) Type() QType {
	return QType{Kind: QKindInvalid}
}

func (v QValueInvalid) Value() any {
	return v.Val
}

type QValueFloat32 struct {
	Val float32
}

func (QValueFloat32) Type() QType {
	return QType{Kind: QKindFloat32}
}

func (v QValueFloat32) Value() any {
	return v.Val
}

type QValueFloat64 struct {
	Val float64
}

func (QValueFloat64) Type() QType {
	return QType{Kind: QKindFloat64}
}

func (v QValueFloat64) Value() any {
	return v.Val
}

type QValueInt16 struct {
	Val int16
}

func (QValueInt16) Type() QType {
	return QType{Kind: QKindInt16}
}

func (v QValueInt16) Value() any {
	return v.Val
}

type QValueInt32 struct {
	Val int32
}

func (QValueInt32) Type() QType {
	return QType{Kind: QKindInt32}
}

func (v QValueInt32) Value() any {
	return v.Val
}

type QValueInt64 struct {
	Val int64
}

func (QValueInt64) Type() QType {
	return QType{Kind: QKindInt64}
}

func (v QValueInt64) Value() any {
	return v.Val
}

type QValueBoolean struct {
	Val bool
}

func (QValueBoolean) Type() QType {
	return QType{Kind: QKindBoolean}
}

func (v QValueBoolean) Value() any {
	return v.Val
}

type QValueStruct struct {
	Val map[string]interface{}
}

func (QValueStruct) Type() QType {
	return QType{Kind: QKindStruct}
}

func (v QValueStruct) Value() any {
	return v.Val
}

type QValueQChar struct {
	Val uint8
}

func (QValueQChar) Type() QType {
	return QType{Kind: QKindQChar}
}

func (v QValueQChar) Value() any {
	return v.Val
}

type QValueString struct {
	Val string
}

func (QValueString) Type() QType {
	return QType{Kind: QKindString}
}

func (v QValueString) Value() any {
	return v.Val
}

type QValueTimestamp struct {
	Val time.Time
}

func (QValueTimestamp) Type() QType {
	return QType{Kind: QKindTimestamp}
}

func (v QValueTimestamp) Value() any {
	return v.Val
}

type QValueTimestampTZ struct {
	Val time.Time
}

func (QValueTimestampTZ) Type() QType {
	return QType{Kind: QKindTimestampTZ}
}

func (v QValueTimestampTZ) Value() any {
	return v.Val
}

type QValueDate struct {
	Val time.Time
}

func (QValueDate) Type() QType {
	return QType{Kind: QKindDate}
}

func (v QValueDate) Value() any {
	return v.Val
}

type QValueTime struct {
	Val time.Time
}

func (QValueTime) Type() QType {
	return QType{Kind: QKindTime}
}

func (v QValueTime) Value() any {
	return v.Val
}

type QValueTimeTZ struct {
	Val time.Time
}

func (QValueTimeTZ) Type() QType {
	return QType{Kind: QKindTimeTZ}
}

func (v QValueTimeTZ) Value() any {
	return v.Val
}

type QValueInterval struct {
	Val string
}

func (QValueInterval) Type() QType {
	return QType{Kind: QKindInterval}
}

func (v QValueInterval) Value() any {
	return v.Val
}

type QValueNumeric struct {
	Val decimal.Decimal
}

func (QValueNumeric) Type() QType {
	return QType{Kind: QKindNumeric}
}

func (v QValueNumeric) Value() any {
	return v.Val
}

type QValueBytes struct {
	Val []byte
}

func (QValueBytes) Type() QType {
	return QType{Kind: QKindBytes}
}

func (v QValueBytes) Value() any {
	return v.Val
}

type QValueUUID struct {
	Val [16]byte
}

func (QValueUUID) Type() QType {
	return QType{Kind: QKindUUID}
}

func (v QValueUUID) Value() any {
	return v.Val
}

type QValueJSON struct {
	Val string
}

func (QValueJSON) Type() QType {
	return QType{Kind: QKindJSON}
}

func (v QValueJSON) Value() any {
	return v.Val
}

type QValueBit struct {
	Val []byte
}

func (QValueBit) Type() QType {
	return QType{Kind: QKindBit}
}

func (v QValueBit) Value() any {
	return v.Val
}

type QValueHStore struct {
	Val string
}

func (QValueHStore) Type() QType {
	return QType{Kind: QKindHStore}
}

func (v QValueHStore) Value() any {
	return v.Val
}

type QValueGeography struct {
	Val string
}

func (QValueGeography) Type() QType {
	return QType{Kind: QKindGeography}
}

func (v QValueGeography) Value() any {
	return v.Val
}

type QValueGeometry struct {
	Val string
}

func (QValueGeometry) Type() QType {
	return QType{Kind: QKindGeometry}
}

func (v QValueGeometry) Value() any {
	return v.Val
}

type QValuePoint struct {
	Val string
}

func (QValuePoint) Type() QType {
	return QType{Kind: QKindPoint}
}

func (v QValuePoint) Value() any {
	return v.Val
}

type QValueCIDR struct {
	Val string
}

func (QValueCIDR) Type() QType {
	return QType{Kind: QKindCIDR}
}

func (v QValueCIDR) Value() any {
	return v.Val
}

type QValueINET struct {
	Val string
}

func (QValueINET) Type() QType {
	return QType{Kind: QKindINET}
}

func (v QValueINET) Value() any {
	return v.Val
}

type QValueMacaddr struct {
	Val string
}

func (QValueMacaddr) Type() QType {
	return QType{Kind: QKindMacaddr}
}

func (v QValueMacaddr) Value() any {
	return v.Val
}

type QValueArrayFloat32 struct {
	Val []float32
}

func (QValueArrayFloat32) Type() QType {
	return QType{
		Kind:  QKindFloat32,
		Array: 1,
	}
}

func (v QValueArrayFloat32) Value() any {
	return v.Val
}

type QValueArrayFloat64 struct {
	Val []float64
}

func (QValueArrayFloat64) Type() QType {
	return QType{
		Kind:  QKindFloat64,
		Array: 1,
	}
}

func (v QValueArrayFloat64) Value() any {
	return v.Val
}

type QValueArrayInt16 struct {
	Val []int16
}

func (QValueArrayInt16) Type() QType {
	return QType{Kind: QKindInt16}
}

func (v QValueArrayInt16) Value() any {
	return v.Val
}

type QValueArrayInt32 struct {
	Val []int32
}

func (QValueArrayInt32) Type() QType {
	return QType{Kind: QKindInt32}
}

func (v QValueArrayInt32) Value() any {
	return v.Val
}

type QValueArrayInt64 struct {
	Val []int64
}

func (QValueArrayInt64) Type() QType {
	return QType{
		Kind:  QKindInt64,
		Array: 1,
	}
}

func (v QValueArrayInt64) Value() any {
	return v.Val
}

type QValueArrayString struct {
	Val []string
}

func (QValueArrayString) Type() QType {
	return QType{
		Kind:  QKindString,
		Array: 1,
	}
}

func (v QValueArrayString) Value() any {
	return v.Val
}

type QValueArrayDate struct {
	Val []time.Time
}

func (QValueArrayDate) Type() QType {
	return QType{
		Kind:  QKindDate,
		Array: 1,
	}
}

func (v QValueArrayDate) Value() any {
	return v.Val
}

type QValueArrayTimestamp struct {
	Val []time.Time
}

func (QValueArrayTimestamp) Type() QType {
	return QType{
		Kind:  QKindTimestamp,
		Array: 1,
	}
}

func (v QValueArrayTimestamp) Value() any {
	return v.Val
}

type QValueArrayTimestampTZ struct {
	Val []time.Time
}

func (QValueArrayTimestampTZ) Type() QType {
	return QType{
		Kind:  QKindTimestampTZ,
		Array: 1,
	}
}

func (v QValueArrayTimestampTZ) Value() any {
	return v.Val
}

type QValueArrayBoolean struct {
	Val []bool
}

func (QValueArrayBoolean) Type() QType {
	return QType{
		Kind:  QKindBoolean,
		Array: 1,
	}
}

func (v QValueArrayBoolean) Value() any {
	return v.Val
}
