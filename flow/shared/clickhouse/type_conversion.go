package clickhouse

import (
	"github.com/PeerDB-io/peerdb/flow/shared/qvalue"
)

/*
This file handles the mapping for ClickHouse destination types and
their corresponding TypeConversion implementations. A TypeConversion
object contains two functions: one for schema conversion (QField) and
one for value conversion (QValue). This allows the avro writer to
stage the schema/data in the converted type format, and therefore
successfully uploaded to the desired destination type in ClickHouse.

To add a type conversion:
	(1) In flow/model/qvalue/type_converter.go:
	- implement a SchemaConversionFn interface to convert the QField type
	- implement a ValueConversionFn interface to convert the QValue data

	(2) Add the new conversion to the `supportedDestinationTypes` map here
		(if destination type doesn't exist, create a new map entry for it).

The ListSupportedTypeConversions function returns the full list of supported
type conversions. Note that the source types are QValueKind, this allows
the implementation to be source-connector agnostic.
*/

var SupportedDestinationTypes = map[string][]qvalue.TypeConversion{
	"String": {qvalue.NewTypeConversion(
		qvalue.NumericToStringSchemaConversion,
		qvalue.NumericToStringValueConversion,
	)},
}

func ListSupportedTypeConversions() map[qvalue.QValueKind][]string {
	typeConversions := make(map[qvalue.QValueKind][]string)

	for dstType, l := range SupportedDestinationTypes {
		for _, conversion := range l {
			typeConversions[conversion.FromKind()] = append(typeConversions[conversion.FromKind()], dstType)
		}
	}
	return typeConversions
}
