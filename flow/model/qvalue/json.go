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

// ShouldWrapNonObjectJSON reports whether a source reader should pass its JSON values through
// WrapNonObjectJSON: the mirror is at or above InternalVersion_WrapNonObjectJSON and its destination maps
// JSON columns to native ClickHouse JSON, which GetFlags records as Flag_ClickHouseJSONEnabled at creation.
func ShouldWrapNonObjectJSON(flags []string, version uint32) bool {
	return version >= shared.InternalVersion_WrapNonObjectJSON && slices.Contains(flags, shared.Flag_ClickHouseJSONEnabled)
}
