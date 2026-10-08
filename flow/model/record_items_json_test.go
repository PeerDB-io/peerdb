package model

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func assertRecordJSONCompatible(t *testing.T, r RecordItems, opts ToJSONOptions) {
	t.Helper()
	m, oldErr := r.toMap(opts)
	var want []byte
	if oldErr == nil {
		want, oldErr = json.Marshal(m)
	}
	got, err := r.MarshalJSONWithOptions(opts)
	if oldErr != nil {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestRecordJSONCompatibility(t *testing.T) {
	kinds := []types.QValueKind{
		types.QValueKindFloat32,
		types.QValueKindFloat64,
		types.QValueKindInt8,
		types.QValueKindInt16,
		types.QValueKindInt32,
		types.QValueKindInt64,
		types.QValueKindInt256,
		types.QValueKindUInt8,
		types.QValueKindUInt16,
		types.QValueKindUInt32,
		types.QValueKindUInt64,
		types.QValueKindUInt256,
		types.QValueKindBoolean,
		types.QValueKindQChar,
		types.QValueKindString,
		types.QValueKindEnum,
		types.QValueKindUint16Enum,
		types.QValueKindUint64Set,
		types.QValueKindTimestamp,
		types.QValueKindTimestampTZ,
		types.QValueKindDate,
		types.QValueKindTime,
		types.QValueKindTimeTZ,
		types.QValueKindInterval,
		types.QValueKindNumeric,
		types.QValueKindBytes,
		types.QValueKindUUID,
		types.QValueKindJSON,
		types.QValueKindJSONB,
		types.QValueKindHStore,
		types.QValueKindGeography,
		types.QValueKindGeometry,
		types.QValueKindPoint,
		types.QValueKindCIDR,
		types.QValueKindINET,
		types.QValueKindMacaddr,
		types.QValueKindArrayFloat32,
		types.QValueKindArrayFloat64,
		types.QValueKindArrayInt16,
		types.QValueKindArrayInt32,
		types.QValueKindArrayInt64,
		types.QValueKindArrayString,
		types.QValueKindArrayEnum,
		types.QValueKindArrayDate,
		types.QValueKindArrayTime,
		types.QValueKindArrayInterval,
		types.QValueKindArrayTimestamp,
		types.QValueKindArrayTimestampTZ,
		types.QValueKindArrayBoolean,
		types.QValueKindArrayJSON,
		types.QValueKindArrayJSONB,
		types.QValueKindArrayUUID,
		types.QValueKindArrayNumeric,
	}
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			for _, value := range []types.QValue{kind.DefaultValue(), types.QValueNull(kind)} {
				assertRecordJSONCompatible(t, RecordItems{ColToVal: map[string]types.QValue{"key": value}}, NewToJSONOptions(nil, true))
			}
		})
	}
	for _, values := range []map[string]types.QValue{
		nil,
		{},
		{"nil": nil},
		{
			"é": types.QValueInt64{Val: 1}, "雪": types.QValueInt64{Val: 2},
			"\ue000": types.QValueInt64{Val: 3}, "😀": types.QValueInt64{Val: 4},
			"\ufffd": types.QValueInt64{Val: 5}, "\xff": types.QValueInt64{Val: 6}, "\xfe": types.QValueInt64{Val: 7},
		},
	} {
		assertRecordJSONCompatible(t, RecordItems{ColToVal: values}, NewToJSONOptions(nil, true))
	}
	for _, f := range []float64{
		0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1),
		math.SmallestNonzeroFloat64, math.MaxFloat64, 1e-7, 1e-6, 1e20, 1e21,
	} {
		assertRecordJSONCompatible(t, RecordItems{ColToVal: map[string]types.QValue{
			"f64": types.QValueFloat64{Val: f}, "f32": types.QValueFloat32{Val: float32(f)},
			"a64": types.QValueArrayFloat64{Val: []float64{f}}, "a32": types.QValueArrayFloat32{Val: []float32{float32(f)}},
		}}, NewToJSONOptions(nil, true))
	}
	for _, clear := range []int{0, 1, 5, 1000} {
		for _, hstore := range []bool{false, true} {
			opts := NewToJSONOptions(nil, hstore)
			opts.ClearValuesOverBytes = clear
			for _, h := range []string{`"a"=>"<b>", "nil"=>NULL`, "not hstore"} {
				assertRecordJSONCompatible(t, RecordItems{ColToVal: map[string]types.QValue{
					"\xff":     types.QValueString{Val: "<>&\u2028\u2029\x00\xff"},
					"\xfe":     types.QValueJSON{Val: `{"x":"<>&"}`},
					"h":        types.QValueHStore{Val: h},
					"bytes":    types.QValueBytes{Val: []byte{0, 255, 42}},
					"strings":  types.QValueArrayString{Val: []string{"<>&", "\xff"}},
					"dates":    types.QValueArrayDate{Val: []time.Time{time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("x", 19800))}},
					"times":    types.QValueArrayTime{Val: []time.Duration{-26 * time.Hour, 0, 25*time.Hour + time.Microsecond}},
					"decimals": types.QValueArrayNumeric{Val: []decimal.Decimal{decimal.New(123, -2), decimal.New(-1, 50)}},
				}}, opts)
			}
		}
	}
}

func TestRecordJSONRejectsUnnest(t *testing.T) {
	_, err := NewRecordItems(0).MarshalJSONWithOptions(NewToJSONOptions([]string{"payload"}, false))
	require.ErrorContains(t, err, "unnesting is not supported")
}

func FuzzRecordJSONCompatibility(f *testing.F) {
	for _, seed := range []string{"", "normal", "<>&\u2028\u2029\xff\xfe", "\"\\\x00\n"} {
		f.Add(seed, seed, uint64(1), int64(0), uint8(0))
	}
	f.Fuzz(func(t *testing.T, key, value string, bits uint64, sec int64, clear uint8) {
		ts := time.Unix(sec%1000000000000, int64(bits%1000000000)).In(time.FixedZone("fuzz", int(int16(bits))))
		r := RecordItems{ColToVal: map[string]types.QValue{
			"s" + key: types.QValueString{Val: value}, "j" + key: types.QValueJSON{Val: value},
			"i": types.QValueInt64{Val: int64(bits)}, "u": types.QValueUInt64{Val: bits},
			"f": types.QValueFloat64{Val: math.Float64frombits(bits)}, "f32": types.QValueFloat32{Val: math.Float32frombits(uint32(bits))},
			"b": types.QValueBytes{Val: []byte(value)}, "c": types.QValueQChar{Val: uint8(bits)},
			"t": types.QValueTimestamp{Val: ts}, "tz": types.QValueTimestampTZ{Val: ts}, "d": types.QValueDate{Val: ts},
			"time": types.QValueTime{Val: time.Duration(sec)}, "timetz": types.QValueTimeTZ{Val: time.Duration(sec)},
			"n":       types.QValueNumeric{Val: decimal.New(int64(bits), int32(int8(clear)))},
			"arr":     types.QValueArrayFloat64{Val: []float64{math.Float64frombits(bits), math.NaN()}},
			"strings": types.QValueArrayString{Val: []string{key, value}},
			"h":       types.QValueHStore{Val: `"a"=>"b"`},
		}}
		r.AddColumn(key, types.QValueString{Val: value})
		r.AddColumn(value, types.QValueInt64{Val: int64(bits)})
		opts := NewToJSONOptions(nil, clear%2 == 0)
		opts.ClearValuesOverBytes = int(clear)
		assertRecordJSONCompatible(t, r, opts)
		assertRecordJSONCompatible(t, RecordItems{ColToVal: map[string]types.QValue{"h": types.QValueHStore{Val: value}}}, opts)
	})
}
