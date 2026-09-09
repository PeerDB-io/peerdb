package structured

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestMalformedDataMarshalJSON(t *testing.T) {
	qv := func(v types.QValue) *types.QValue { return &v }

	tests := []struct {
		setup    func(m *MalformedData)
		desc     string
		expected string
	}{
		{
			desc:     "empty",
			setup:    func(*MalformedData) {},
			expected: `{"malformed_data":{}}`,
		},
		{
			desc:     "reason without value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonUnexpected, nil) },
			expected: `{"malformed_data":{"a":{"unexpected":true}}}`,
		},
		{
			desc:     "not a number is a reason without value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonNaN, nil) },
			expected: `{"malformed_data":{"a":{"not_a_number":true}}}`,
		},
		{
			desc:     "string value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonTypeMismatch, qv(types.QValueString{Val: "x"})) },
			expected: `{"malformed_data":{"a":{"type_mismatch":true,"value":"x"}}}`,
		},
		{
			desc:     "JSON value is kept as a quoted string",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonTypeMismatch, qv(types.QValueJSON{Val: `{"k":1}`})) },
			expected: `{"malformed_data":{"a":{"type_mismatch":true,"value":"{\"k\":1}"}}}`,
		},
		{
			desc: "null value",
			setup: func(m *MalformedData) {
				m.AddField("a", ReasonTypeMismatch, qv(types.QValueNull(types.QValueKindInt64)))
			},
			expected: `{"malformed_data":{"a":{"type_mismatch":true,"value":null}}}`,
		},
		{
			desc: "scalar values are encoded natively",
			setup: func(m *MalformedData) {
				m.AddField("i", ReasonTypeMismatch, qv(types.QValueInt64{Val: 1}))
				m.AddField("b", ReasonTypeMismatch, qv(types.QValueBoolean{Val: true}))
				m.AddField("f", ReasonTypeMismatch, qv(types.QValueFloat64{Val: 1.5}))
			},
			expected: `{"malformed_data":{"b":{"type_mismatch":true,"value":true},"f":{"type_mismatch":true,"value":1.5},"i":{"type_mismatch":true,"value":1}}}`,
		},
		{
			desc: "value is overwritten by a later AddField for the same field",
			setup: func(m *MalformedData) {
				m.AddField("a", ReasonUnexpected, qv(types.QValueString{Val: "x"}))
				m.AddField("a", ReasonTypeMismatch, qv(types.QValueString{Val: "y"}))
			},
			expected: `{"malformed_data":{"a":{"type_mismatch":true,"value":"y"}}}`,
		},
		{
			desc: "non-finite float values are dropped and the reason replaced by not_a_number",
			setup: func(m *MalformedData) {
				m.AddField("nan", ReasonTypeMismatch, qv(types.QValueFloat64{Val: math.NaN()}))
				m.AddField("inf", ReasonUnexpected, qv(types.QValueFloat64{Val: math.Inf(1)}))
				m.AddField("neginf", ReasonTypeMismatch, qv(types.QValueFloat64{Val: math.Inf(-1)}))
				m.AddField("nan32", ReasonTypeMismatch, qv(types.QValueFloat32{Val: float32(math.NaN())}))
				m.AddField("finite", ReasonTypeMismatch, qv(types.QValueFloat64{Val: 1.5}))
			},
			expected: `{"malformed_data":{` +
				`"finite":{"type_mismatch":true,"value":1.5},` +
				`"inf":{"not_a_number":true},` +
				`"nan":{"not_a_number":true},` +
				`"nan32":{"not_a_number":true},` +
				`"neginf":{"not_a_number":true}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			m := NewMalformedData()
			test.setup(m)
			// marshal through encoding/json to also prove the Marshaler is picked up via the pointer
			b, err := json.Marshal(m)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, string(b))
			require.Equal(t, test.expected, string(b), "output must be deterministic")
		})
	}

	t.Run("unknown reason is an error", func(t *testing.T) {
		m := NewMalformedData()
		m.AddField("a", MalformedReason(99), nil)
		_, err := json.Marshal(m)
		require.ErrorContains(t, err, `"a"`)
		require.ErrorContains(t, err, "unknown reason 99")
	})

	t.Run("value without reason is an error", func(t *testing.T) {
		m := NewMalformedData()
		m.AddField("a", ReasonTypeMismatch, nil)
		m.values["b"] = types.QValueString{Val: "x"} // AddField cannot produce this state
		_, err := json.Marshal(m)
		require.ErrorContains(t, err, `"b"`)
		require.ErrorContains(t, err, "no reason")
	})
}
