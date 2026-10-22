package qvalue

import (
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// WrapNonObjectJSON wraps a JSON document that is not an object as {"_value": doc}, since ClickHouse's
// JSON type only accepts objects at the top level. Objects and empty input are returned unchanged.
func WrapNonObjectJSON(doc string) string {
	trimmed := strings.TrimSpace(doc)
	if trimmed == "" || trimmed[0] == '{' {
		return doc
	}
	return `{"_value":` + doc + `}`
}

// ShouldWrapNonObjectJSON reports whether a source reader wraps a JSON column's values: the mirror is at or
// above InternalVersion_WrapNonObjectJSON and the column lands in native ClickHouse JSON, either through an
// explicit destination type of JSON or Nullable(JSON), or, without one, through Flag_ClickHouseJSONEnabled,
// which GetFlags records at creation when the destination maps JSON columns to native JSON.
func ShouldWrapNonObjectJSON(flags []string, version uint32, destinationType string) bool {
	if version < shared.InternalVersion_WrapNonObjectJSON {
		return false
	}
	if destinationType != "" {
		return destinationType == "JSON" || destinationType == "Nullable(JSON)"
	}
	return slices.Contains(flags, shared.Flag_ClickHouseJSONEnabled)
}
