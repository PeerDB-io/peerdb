package connbigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func getBigQueryTypeForGenericColumnType(colType string) bigquery.FieldType {
	switch qvalue.QValueKind(colType) {
	// boolean
	case qvalue.QValueKindBoolean:
		return bigquery.BooleanFieldType
	// integer types
	case qvalue.QValueKindInt16, qvalue.QValueKindInt32, qvalue.QValueKindInt64:
		return bigquery.IntegerFieldType
	// decimal types
	case qvalue.QValueKindFloat16, qvalue.QValueKindFloat32, qvalue.QValueKindFloat64:
		return bigquery.FloatFieldType
	case qvalue.QValueKindNumeric:
		return bigquery.NumericFieldType
	// string related
	case qvalue.QValueKindString:
		return bigquery.StringFieldType
	// json also is stored as string for now
	case qvalue.QValueKindJSON:
		return bigquery.StringFieldType
	// time related
	case qvalue.QValueKindETime:
		return bigquery.TimestampFieldType
	// TODO: handle INTERVAL types again
	// bytes
	case qvalue.QValueKindBit, qvalue.QValueKindBytes:
		return bigquery.BytesFieldType
	// rest will be strings
	default:
		return bigquery.StringFieldType
	}
}
