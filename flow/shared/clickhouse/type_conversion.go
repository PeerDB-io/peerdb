package clickhouse

import (
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

/*
This file handles the mapping for ClickHouse destination types and
their corresponding TypeConversion implementations. A TypeConversion
object contains two functions: one for schema conversion (QField) and
one for value conversion (QValue). This allows the avro writer to
stage the schema/data in the converted type format, and therefore
successfully uploaded to the desired destination type in ClickHouse.

To add a type conversion:

	(1) In flow/model/shared/type_converter.go:
	- implement a SchemaConversionFn interface to convert the QField type
	- implement a ValueConversionFn interface to convert the QValue data

	(2) Add the new conversion to the `supportedDestinationTypes` map here
		(if destination type doesn't exist, create a new map entry for it).
*/
var SupportedDestinationTypes = map[string][]types.TypeConversion{
	"String": {
		types.NewTypeConversion(
			types.NumericToStringSchemaConversion,
			types.NumericToStringValueConversion,
		),
	},
	"Int256": {
		types.NewTypeConversion(
			types.NumericToInt256SchemaConversion,
			types.NumericToInt256ValueConversion,
		),
	},
	"UInt256": {
		types.NewTypeConversion(
			types.NumericToUInt256SchemaConversion,
			types.NumericToUInt256ValueConversion,
		),
	},
}

var NumericDestinationTypes = map[string]struct{}{
	"String":  {},
	"Int256":  {},
	"UInt256": {},
}

// returns the full list of supported type conversions. The keys are
// QValueKind to allows the implementation to be source-connector agnostic.
func ListSupportedTypeConversions() map[types.QValueKind][]string {
	typeConversions := make(map[types.QValueKind][]string)

	for dstType, l := range SupportedDestinationTypes {
		for _, conversion := range l {
			typeConversions[conversion.FromKind()] = append(typeConversions[conversion.FromKind()], dstType)
		}
	}
	return typeConversions
}
