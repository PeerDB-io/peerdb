package qvalue

import (
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// WrapNonObjectJSON wraps a JSON document that is not an object as {"_value": doc}, since ClickHouse's
// JSON type only accepts objects at the top level. Objects and empty input are returned unchanged.
//
// doc is JSON text as Postgres and MySQL emit it, so its first non-blank byte identifies the kind: an
// object always starts with '{' and nothing else can, since a top-level string is quoted
// (the string value {a is the document "{a").
func WrapNonObjectJSON(doc string) string {
	trimmed := strings.TrimSpace(doc)
	if trimmed == "" || trimmed[0] == '{' {
		return doc
	}
	return `{"_value":` + doc + `}`
}

// ConvertJSONTypeToUppercase returns JSON, Nullable(JSON), Array(JSON) or Array(Nullable(JSON)) when
// destinationType is one of them written in another case, like Array(json), which ClickHouse accepts and
// stores as Array(JSON). Any other type is returned unchanged.
func ConvertJSONTypeToUppercase(destinationType string) string {
	for _, jsonType := range []string{"JSON", "Nullable(JSON)", "Array(JSON)", "Array(Nullable(JSON))"} {
		if strings.EqualFold(destinationType, jsonType) {
			return jsonType
		}
	}
	return destinationType
}

// ShouldWrapNonObjectJSON reports whether a JSON column with the given explicit destination type, "" for
// none, lands in native ClickHouse JSON on this mirror and so has its non-object values wrapped. nativeJSONEnabled
// is PEERDB_CLICKHOUSE_ENABLE_JSON, which the readers look up once per pull.
func ShouldWrapNonObjectJSON(flags []string, version uint32, nativeJSONEnabled bool, destinationType string) bool {
	if version < shared.InternalVersion_WrapNonObjectJSON {
		return false
	}
	switch ConvertJSONTypeToUppercase(destinationType) {
	case "":
		return nativeJSONEnabled && slices.Contains(flags, shared.Flag_ClickHouseJSONSupported)
	case "JSON", "Nullable(JSON)":
		return slices.Contains(flags, shared.Flag_ClickHouseJSONSupported)
	default:
		return false
	}
}

// ShouldWrapJSONArrayElements is ShouldWrapNonObjectJSON for the elements of a json[]/jsonb[] column.
func ShouldWrapJSONArrayElements(flags []string, version uint32, nativeJSONEnabled bool, destinationType string) bool {
	if version < shared.InternalVersion_WrapJSONArrayElements || !slices.Contains(flags, shared.Flag_ClickHouseJSONSupported) {
		return false
	}
	switch ConvertJSONTypeToUppercase(destinationType) {
	case "":
		return version >= shared.InternalVersion_JSONArrayAsArrayJSON && nativeJSONEnabled
	case "Array(JSON)", "Array(Nullable(JSON))":
		return true
	default:
		return false
	}
}
