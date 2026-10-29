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
	clickhouse := []string{shared.Flag_ClickHouseTime64Enabled, shared.Flag_ClickHouseJSONSupported}
	otherDestination := []string{shared.Flag_ClickHouseTime64Enabled}

	for _, tc := range []struct {
		destinationType   string
		flags             []string
		version           uint32
		nativeJSONEnabled bool
		want              bool
	}{
		{flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON, nativeJSONEnabled: true, want: true},
		{flags: clickhouse, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, want: true},
		{flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON - 1, nativeJSONEnabled: true, want: false},
		{flags: clickhouse, version: shared.InternalVersion_Latest, want: false},
		{flags: otherDestination, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, want: false},
		{flags: nil, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, want: false},
		// an explicit destination type decides for its column, whatever the mirror default is
		{flags: clickhouse, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, destinationType: "String", want: false},
		{flags: clickhouse, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, destinationType: "Nullable(String)", want: false},
		{flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "JSON", want: true},
		{flags: clickhouse, version: shared.InternalVersion_Latest, destinationType: "Nullable(JSON)", want: true},
		{flags: clickhouse, version: shared.InternalVersion_WrapNonObjectJSON - 1, destinationType: "JSON", want: false},
		// an explicit JSON type is only honoured where the destination has the type
		{flags: otherDestination, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, destinationType: "JSON", want: false},
		{flags: nil, version: shared.InternalVersion_Latest, nativeJSONEnabled: true, destinationType: "Nullable(JSON)", want: false},
	} {
		got := ShouldWrapNonObjectJSON(tc.flags, tc.version, tc.nativeJSONEnabled, tc.destinationType)
		require.Equal(t, tc.want, got, "flags=%v version=%d nativeJSONEnabled=%t destinationType=%q",
			tc.flags, tc.version, tc.nativeJSONEnabled, tc.destinationType)
	}
}
