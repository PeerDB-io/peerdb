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

// ShouldWrapNonObjectJSON reports whether a JSON column with the given explicit destination type, "" for
// none, lands in native ClickHouse JSON on this mirror and so has its non-object values wrapped.
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
