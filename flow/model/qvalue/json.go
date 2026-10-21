package qvalue

import (
	"context"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/internal"
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
// WrapNonObjectJSON: the mirror is at or above InternalVersion_WrapNonObjectJSON and
// PEERDB_CLICKHOUSE_ENABLE_JSON is on.
func ShouldWrapNonObjectJSON(ctx context.Context, env map[string]string, version uint32) (bool, error) {
	if version < shared.InternalVersion_WrapNonObjectJSON {
		return false, nil
	}
	return internal.PeerDBEnableClickHouseJSON(ctx, env)
}
