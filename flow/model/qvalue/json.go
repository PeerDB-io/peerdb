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

// ShouldWrapNonObjectJSON reports whether a JSON column with the given explicit destination type, "" for
// none, lands in native ClickHouse JSON on this mirror and so has its non-object values wrapped. nativeJSONEnabled
// is PEERDB_CLICKHOUSE_ENABLE_JSON, which the readers look up once per pull.
func ShouldWrapNonObjectJSON(flags []string, version uint32, nativeJSONEnabled bool, destinationType string) bool {
	if version < shared.InternalVersion_WrapNonObjectJSON {
		return false
	}
	switch destinationType {
	case "":
		return nativeJSONEnabled && slices.Contains(flags, shared.Flag_ClickHouseJSONSupported)
	case "JSON", "Nullable(JSON)":
		return slices.Contains(flags, shared.Flag_ClickHouseJSONSupported)
	default:
		return false
	}
}
