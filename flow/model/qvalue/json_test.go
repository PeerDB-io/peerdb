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
	nativeJSON := []string{shared.Flag_ClickHouseTime64Enabled, shared.Flag_ClickHouseJSONEnabled}
	noNativeJSON := []string{shared.Flag_ClickHouseTime64Enabled}

	for _, tc := range []struct {
		flags   []string
		version uint32
		want    bool
	}{
		{nativeJSON, shared.InternalVersion_WrapNonObjectJSON, true},
		{nativeJSON, shared.InternalVersion_Latest, true},
		{nativeJSON, shared.InternalVersion_WrapNonObjectJSON - 1, false},
		{noNativeJSON, shared.InternalVersion_Latest, false},
		{nil, shared.InternalVersion_Latest, false},
	} {
		require.Equal(t, tc.want, ShouldWrapNonObjectJSON(tc.flags, tc.version), "flags=%v version=%d", tc.flags, tc.version)
	}
}
