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
// above InternalVersion_WrapNonObjectJSON and the column lands in native ClickHouse JSON. An explicit
// destination type decides for its column, JSON or Nullable(JSON) on a destination that has the type
// (Flag_ClickHouseJSONSupported); without one the mirror default decides (Flag_ClickHouseJSONEnabled).
// GetFlags records both flags at mirror creation.
func ShouldWrapNonObjectJSON(flags []string, version uint32, destinationType string) bool {
	if version < shared.InternalVersion_WrapNonObjectJSON {
		return false
	}
	switch destinationType {
	case "":
		return slices.Contains(flags, shared.Flag_ClickHouseJSONEnabled)
	case "JSON", "Nullable(JSON)":
		return slices.Contains(flags, shared.Flag_ClickHouseJSONSupported)
	default:
		return false
	}
}
