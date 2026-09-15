package structured

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestMalformedDataMarshalJSON(t *testing.T) {
	tests := []struct {
		setup    func(m *MalformedData)
		desc     string
		expected string
	}{
		{
			desc:     "empty",
			setup:    func(*MalformedData) {},
			expected: `{}`,
		},
		{
			desc:     "reason without value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonUnexpected, nil) },
			expected: `{"a":{"unexpected_field":true}}`,
		},
		{
			desc:     "not a number is a reason without value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonNaN, nil) },
			expected: `{"a":{"not_a_number":true}}`,
		},
		{
			desc:     "string value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonTypeMismatch, types.QValueString{Val: "x"}) },
			expected: `{"a":{"type_mismatch":true,"value":"x"}}`,
		},
		{
			desc:     "duplicated field with value",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonDuplicatedFields, types.QValueString{Val: "x"}) },
			expected: `{"a":{"duplicated_fields":true,"value":"x"}}`,
		},
		{
			desc:     "JSON value is kept as a quoted string",
			setup:    func(m *MalformedData) { m.AddField("a", ReasonTypeMismatch, types.QValueJSON{Val: `{"k":1}`}) },
			expected: `{"a":{"type_mismatch":true,"value":"{\"k\":1}"}}`,
		},
		{
			desc: "null value",
			setup: func(m *MalformedData) {
				m.AddField("a", ReasonTypeMismatch, types.QValueNull(types.QValueKindInt64))
			},
			expected: `{"a":{"type_mismatch":true,"value":null}}`,
		},
		{
			desc: "scalar values are encoded natively",
			setup: func(m *MalformedData) {
				m.AddField("i", ReasonTypeMismatch, types.QValueInt64{Val: 1})
				m.AddField("b", ReasonTypeMismatch, types.QValueBoolean{Val: true})
				m.AddField("f", ReasonTypeMismatch, types.QValueFloat64{Val: 1.5})
			},
			expected: `{"b":{"type_mismatch":true,"value":true},"f":{"type_mismatch":true,"value":1.5},"i":{"type_mismatch":true,"value":1}}`,
		},
		{
			desc: "value is overwritten by a later AddField for the same field",
			setup: func(m *MalformedData) {
				m.AddField("a", ReasonUnexpected, types.QValueString{Val: "x"})
				m.AddField("a", ReasonTypeMismatch, types.QValueString{Val: "y"})
			},
			expected: `{"a":{"type_mismatch":true,"value":"y"}}`,
		},
		{
			desc: "non-finite float values are dropped and the reason replaced by not_a_number",
			setup: func(m *MalformedData) {
				m.AddField("nan", ReasonTypeMismatch, types.QValueFloat64{Val: math.NaN()})
				m.AddField("inf", ReasonUnexpected, types.QValueFloat64{Val: math.Inf(1)})
				m.AddField("neginf", ReasonTypeMismatch, types.QValueFloat64{Val: math.Inf(-1)})
				m.AddField("nan32", ReasonTypeMismatch, types.QValueFloat32{Val: float32(math.NaN())})
				m.AddField("finite", ReasonTypeMismatch, types.QValueFloat64{Val: 1.5})
			},
			expected: `{` +
				`"finite":{"type_mismatch":true,"value":1.5},` +
				`"inf":{"not_a_number":true},` +
				`"nan":{"not_a_number":true},` +
				`"nan32":{"not_a_number":true},` +
				`"neginf":{"not_a_number":true}}`,
		},
		{
			desc: "compound values holding non-finite floats fall back to their string representation",
			setup: func(m *MalformedData) {
				m.AddField("arr64", ReasonTypeMismatch, types.QValueArrayFloat64{Val: []float64{1, math.NaN()}})
				m.AddField("arr32", ReasonUnexpected, types.QValueArrayFloat32{Val: []float32{float32(math.Inf(1))}})
				m.AddField("finite", ReasonTypeMismatch, types.QValueArrayFloat64{Val: []float64{1, 2}})
			},
			expected: `{` +
				`"arr32":{"unexpected_field":true,"value":"[+Inf]"},` +
				`"arr64":{"type_mismatch":true,"value":"[1 NaN]"},` +
				`"finite":{"type_mismatch":true,"value":[1,2]}}`,
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
