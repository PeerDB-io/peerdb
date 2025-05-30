package connclickhouse

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

/*
This file handles the mapping for ClickHouse destination types and
their corresponding TypeConversion implementations. A TypeConversion
object contains two functions: one for schema conversion (QField) and
one for value conversion (QValue). This allows the avro writer to
write the schema/data to the desired destination type in ClickHouse.

To add a type conversion:
	(1) In flow/model/qvalue/type_converter.go:
	- implement a SchemaConversionFn interface to convert the QField type
	- implement a ValueConversionFn interface to convert the QValue data

	(2) Add the new conversion to the `supportedDestinationTypes` map here
		(if destination type doesn't exist, create a new map entry for it).

The GetColumnsTypeConversion function returns the full list of supported
type conversions. Note that the source types are QValueKind, this allows
the implementation to be source-connector agnostic.
*/

var supportedDestinationTypes = map[string][]qvalue.TypeConversion{
	"String": {qvalue.NewTypeConversion(
		qvalue.NumericToStringSchemaConversion,
		qvalue.NumericToStringValueConversion,
	)},
}

func GetColumnsTypeConversion() (*protos.ColumnsTypeConversionResponse, error) {
	res := make([]*protos.ColumnsTypeConversion, 0)
	for qkind, destTypes := range listSupportedTypeConversions() {
		res = append(res, &protos.ColumnsTypeConversion{
			Qkind:            string(qkind),
			DestinationTypes: destTypes,
		})
	}
	return &protos.ColumnsTypeConversionResponse{
		Conversions: res,
	}, nil
}

func listSupportedTypeConversions() map[qvalue.QValueKind][]string {
	typeConversions := make(map[qvalue.QValueKind][]string)

	for dstType, l := range supportedDestinationTypes {
		for _, conversion := range l {
			typeConversions[conversion.FromKind()] = append(typeConversions[conversion.FromKind()], dstType)
		}
	}
	return typeConversions
}

func findTypeConversions(schema qvalue.QRecordSchema, columns []*protos.ColumnSetting) map[string]qvalue.TypeConversion {
	typeConversions := make(map[string]qvalue.TypeConversion)

	colNameToType := make(map[string]qvalue.QValueKind, len(schema.Fields))
	for _, field := range schema.Fields {
		colNameToType[field.Name] = field.Type
	}

	for _, col := range columns {
		colType, exist := colNameToType[col.SourceName]
		if !exist {
			continue
		}
		conversions, exist := supportedDestinationTypes[col.DestinationType]
		if !exist {
			continue
		}
		for _, conversion := range conversions {
			if conversion.FromKind() == colType {
				typeConversions[col.SourceName] = conversion
			}
		}
	}

	return typeConversions
}

func applyTypeConversions(schema qvalue.QRecordSchema, typeConversions map[string]qvalue.TypeConversion) qvalue.QRecordSchema {
	for i, field := range schema.Fields {
		if conversion, exist := typeConversions[field.Name]; exist {
			schema.Fields[i] = conversion.SchemaConversion(field)
		}
	}
	return schema
}
