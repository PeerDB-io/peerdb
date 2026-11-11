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
		// a top-level string is quoted, so one that starts with a brace is not taken for an object
		{`"{a"`, `{"_value":"{a"}`},
		{`"{\"a\": 1}"`, `{"_value":"{\"a\": 1}"}`},
		{`true`, `{"_value":true}`},
		{`[1, 2]`, `{"_value":[1, 2]}`},
		{`null`, `{"_value":null}`},
		{` 42`, `{"_value": 42}`},
		{``, ``},
	} {
		require.Equal(t, tc.want, WrapNonObjectJSON(tc.in), "in=%q", tc.in)
	}
}

func TestShouldWrapJSONArrayElements(t *testing.T) {
	// a ClickHouse with the JSON type
	clickhouse := []string{shared.Flag_ClickHouseTime64Enabled, shared.Flag_ClickHouseJSONSupported}
	otherDestination := []string{shared.Flag_ClickHouseTime64Enabled}
	latest := shared.InternalVersion_Latest

	for _, tc := range []struct {
		destinationType   string
		flags             []string
		version           uint32
		nativeJSONEnabled bool
		want              bool
	}{
		// the default mapping: setting on, JSON-capable ClickHouse, from the array version on
		{flags: clickhouse, version: shared.InternalVersion_JSONArrayAsArrayJSON, nativeJSONEnabled: true, want: true},
		{flags: clickhouse, version: latest, nativeJSONEnabled: true, want: true},
		{flags: clickhouse, version: shared.InternalVersion_JSONArrayAsArrayJSON - 1, nativeJSONEnabled: true, want: false},
		{flags: clickhouse, version: latest, want: false},
		{flags: otherDestination, version: latest, nativeJSONEnabled: true, want: false},
		{flags: nil, version: latest, nativeJSONEnabled: true, want: false},
		// an explicit array type selects it from the wrapping version on, whatever the setting
		{flags: clickhouse, version: shared.InternalVersion_WrapJSONArrayElements, destinationType: "Array(JSON)", want: true},
		{flags: clickhouse, version: latest, destinationType: "Array(Nullable(JSON))", want: true},
		{flags: clickhouse, version: shared.InternalVersion_WrapJSONArrayElements - 1, destinationType: "Array(JSON)", want: false},
		{flags: otherDestination, version: latest, destinationType: "Array(JSON)", want: false},
		// other explicit types never wrap
		{flags: clickhouse, version: latest, nativeJSONEnabled: true, destinationType: "String", want: false},
		{flags: clickhouse, version: latest, nativeJSONEnabled: true, destinationType: "Array(String)", want: false},
		{flags: clickhouse, version: latest, nativeJSONEnabled: true, destinationType: "JSON", want: false},
	} {
		got := ShouldWrapJSONArrayElements(tc.flags, tc.version, tc.nativeJSONEnabled, tc.destinationType)
		require.Equal(t, tc.want, got, "flags=%v version=%d nativeJSONEnabled=%t destinationType=%q",
			tc.flags, tc.version, tc.nativeJSONEnabled, tc.destinationType)
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
