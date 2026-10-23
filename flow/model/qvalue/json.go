package qvalue

import (
	"context"
	"fmt"
	"slices"
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

// NonObjectJSONWrapping is what a source reader needs to decide, per column, whether it wraps non-object
// JSON values for the destination. Resolve it once per pull and ask Wraps for each JSON column.
type NonObjectJSONWrapping struct {
	// active is false below InternalVersion_WrapNonObjectJSON, so mirrors from before the wrapping never wrap
	active bool
	// supported means the destination is a ClickHouse that has the native JSON type
	// (Flag_ClickHouseJSONSupported), so an explicit JSON destination type on a column is honoured
	supported bool
	// byDefault means JSON columns without an explicit destination type land in native JSON: the destination
	// has the type and PEERDB_CLICKHOUSE_ENABLE_JSON is on, which is how the destination picks its column types
	byDefault bool
}

// ResolveNonObjectJSONWrapping reads what Wraps needs from the mirror's version, destination flags and settings.
func ResolveNonObjectJSONWrapping(
	ctx context.Context, env map[string]string, flags []string, version uint32,
) (NonObjectJSONWrapping, error) {
	wrapping := NonObjectJSONWrapping{
		active:    version >= shared.InternalVersion_WrapNonObjectJSON,
		supported: slices.Contains(flags, shared.Flag_ClickHouseJSONSupported),
	}
	if wrapping.active && wrapping.supported {
		enabled, err := internal.PeerDBEnableClickHouseJSON(ctx, env)
		if err != nil {
			return NonObjectJSONWrapping{}, fmt.Errorf("failed to read PEERDB_CLICKHOUSE_ENABLE_JSON: %w", err)
		}
		wrapping.byDefault = enabled
	}
	return wrapping, nil
}

// Wraps reports whether a JSON column with the given explicit destination type, "" for none, lands in
// native ClickHouse JSON and so has its non-object values wrapped.
func (w NonObjectJSONWrapping) Wraps(destinationType string) bool {
	if !w.active {
		return false
	}
	switch destinationType {
	case "":
		return w.byDefault
	case "JSON", "Nullable(JSON)":
		return w.supported
	default:
		return false
	}
}
