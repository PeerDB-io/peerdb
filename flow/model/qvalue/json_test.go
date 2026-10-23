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

func TestNonObjectJSONWrapping(t *testing.T) {
	on := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "true"}
	off := map[string]string{"PEERDB_CLICKHOUSE_ENABLE_JSON": "false"}
	clickhouse := []string{shared.Flag_ClickHouseTime64Enabled, shared.Flag_ClickHouseJSONSupported}
	otherDestination := []string{shared.Flag_ClickHouseTime64Enabled}

	for _, tc := range []struct {
		env             map[string]string
		destinationType string
		flags           []string
		version         uint32
		want            bool
	}{
		{env: on, flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON, want: true},
		{env: on, flags: clickhouse, version: shared.InternalVersion_Latest, want: true},
		{env: on, flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON - 1, want: false},
		{env: off, flags: clickhouse, version: shared.InternalVersion_Latest, want: false},
		{env: on, flags: otherDestination, version: shared.InternalVersion_Latest, want: false},
		{env: on, flags: nil, version: shared.InternalVersion_Latest, want: false},
		// an explicit destination type decides for its column, whatever the mirror default is
		{env: on, flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "String", want: false},
		{env: on, flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "Nullable(String)", want: false},
		{env: off, flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "JSON", want: true},
		{env: off, flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "Nullable(JSON)", want: true},
		{env: off, flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON - 1, destinationType: "JSON", want: false},
		// an explicit JSON type is only honoured where the destination has the type
		{env: on, flags: otherDestination, version: shared.InternalVersion_Latest, destinationType: "JSON", want: false},
		{env: on, flags: nil, version: shared.InternalVersion_Latest, destinationType: "Nullable(JSON)", want: false},
	} {
		wrapping, err := ResolveNonObjectJSONWrapping(t.Context(), tc.env, tc.flags, tc.version)
		require.NoError(t, err)
		got := wrapping.Wraps(tc.destinationType)
		require.Equal(t, tc.want, got, "env=%v flags=%v version=%d destinationType=%q",
			tc.env, tc.flags, tc.version, tc.destinationType)
	}
}
