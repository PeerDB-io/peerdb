package connclickhouse

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestEngineToStringExhaustive(t *testing.T) {
	for enumVal := range protos.TableEngine_name {
		_, err := engineToString(protos.TableEngine(enumVal))
		assert.NoErrorf(t, err, "engineToString(%v) failed", protos.TableEngine(enumVal))
	}
}

// TestCheckNativeJSONSetting checks that PEERDB_CLICKHOUSE_ENABLE_JSON is rejected on a ClickHouse without the
// native JSON type instead of silently mapping JSON columns to String.
func TestCheckNativeJSONSetting(t *testing.T) {
	t.Parallel()

	on := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	off := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "false"}
	v25_2 := &chproto.Version{Major: 25, Minor: 2, Patch: 1}
	v25_3 := &chproto.Version{Major: 25, Minor: 3}
	v26_6 := &chproto.Version{Major: 26, Minor: 6}

	require.NoError(t, checkNativeJSONSetting(t.Context(), on, v25_3))
	require.NoError(t, checkNativeJSONSetting(t.Context(), on, v26_6))
	require.NoError(t, checkNativeJSONSetting(t.Context(), off, v25_2))
	require.NoError(t, checkNativeJSONSetting(t.Context(), off, nil))

	err := checkNativeJSONSetting(t.Context(), on, v25_2)
	require.ErrorContains(t, err, "25.3 or newer")
	require.ErrorContains(t, err, "25.2.1")
	err = checkNativeJSONSetting(t.Context(), on, nil)
	require.ErrorContains(t, err, "unknown")
}
