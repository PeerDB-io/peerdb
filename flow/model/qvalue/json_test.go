package qvalue

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestWrapNonObjectJSON(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{`{"a": 1}`, `{"a": 1}`},
		{`{}`, `{}`},
		{" \n\t" + `{"a": 1}`, " \n\t" + `{"a": 1}`},
		{`42`, `{"_value":42}`},
		{`"dark"`, `{"_value":"dark"}`},
		{`true`, `{"_value":true}`},
		{`[1, 2]`, `{"_value":[1, 2]}`},
		{`null`, `{"_value":null}`},
		{` 42`, `{"_value": 42}`},
		{``, ``},
	} {
		require.Equal(t, tc.want, WrapNonObjectJSON(tc.in), "in=%q", tc.in)
	}
}

func TestShouldWrapNonObjectJSON(t *testing.T) {
	ctx := t.Context()
	on := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	off := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "false"}

	for _, tc := range []struct {
		env     map[string]string
		version uint32
		want    bool
	}{
		{on, shared.InternalVersion_WrapNonObjectJSON, true},
		{on, shared.InternalVersion_Latest, true},
		{on, shared.InternalVersion_WrapNonObjectJSON - 1, false},
		{off, shared.InternalVersion_Latest, false},
	} {
		got, err := ShouldWrapNonObjectJSON(ctx, tc.env, tc.version)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "env=%v version=%d", tc.env, tc.version)
	}
}
