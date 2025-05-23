package qvalue

import "github.com/shopspring/decimal"

type TypeConversion struct {
	SchemaConversionFn func(QField) QField
	ValueConversionFn  func(QValue) QValue
	FromKind           QValueKind
	ToKind             QValueKind
}

func NumericToStringSchemaConversion(val QField) QField {
	val.Type = QValueKindString
	val.Nullable = false
	return val
}

func NumericToStringSchemaConversionNullable(val QField) QField {
	val.Type = QValueKindString
	val.Nullable = true
	return val
}

func NumericToStringValueConversion(val QValue) QValue {
	return QValue(QValueString{Val: val.Value().(decimal.Decimal).String()})
}
