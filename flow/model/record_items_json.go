package model

import (
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// MarshalJSONWithOptions preserves encoding/json's wire representation without
// building a second map or asking the generic encoder to rediscover QValue types.
func (r RecordItems) MarshalJSONWithOptions(opts ToJSONOptions) ([]byte, error) {
	if len(opts.UnnestColumns) != 0 {
		return nil, fmt.Errorf("RecordItems JSON unnesting is not supported")
	}
	return jsonv2.Marshal(recordJSON{items: r, opts: opts}, json.DefaultOptionsV1())
}

// Marshal supplies a pooled encoder and owns the output buffer. Using its token
// hook avoids a separate streaming buffer and its trailing newline.
type recordJSON struct {
	items RecordItems
	opts  ToJSONOptions
}

func (r recordJSON) MarshalJSONTo(enc *jsontext.Encoder) error {
	w := recordJSONWriter{enc: enc}
	w.token(jsontext.BeginObject)
	// Sort the original strings, before UTF-8 replacement or escaping, just as
	// encoding/json does. Distinct invalid UTF-8 keys may encode identically;
	// DefaultOptionsV1 deliberately permits those duplicate JSON names.
	for _, col := range slices.Sorted(maps.Keys(r.items.ColToVal)) {
		w.token(jsontext.String(col))
		w.value(r.items.ColToVal[col], r.opts)
		if w.err != nil {
			return fmt.Errorf("serialize column %q: %w", col, w.err)
		}
	}
	w.token(jsontext.EndObject)
	return w.err
}

type recordJSONWriter struct {
	enc *jsontext.Encoder
	err error
}

func (w *recordJSONWriter) token(t jsontext.Token) {
	if w.err == nil {
		w.err = w.enc.WriteToken(t)
	}
}

// These arrays previously passed through make([]T, 0, len(values)), so even a
// nil input must produce [] rather than null.
func (w *recordJSONWriter) array[T any](values []T, token func(T) jsontext.Token) {
	w.token(jsontext.BeginArray)
	for _, value := range values {
		w.token(token(value))
	}
	w.token(jsontext.EndArray)
}

func jsonFloat64(v float64) jsontext.Token {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return jsontext.Null
	}
	return jsontext.Float(v)
}

func jsonFloat32(v float32) jsontext.Token {
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
		return jsontext.Null
	}
	return jsontext.Float32(v)
}

func (w *recordJSONWriter) value(qv types.QValue, opts ToJSONOptions) {
	switch v := qv.(type) {
	case nil, types.QValueNull:
		w.token(jsontext.Null)
	case types.QValueString:
		value := v.Val
		if opts.ClearValuesOverBytes > 0 && len(value) > opts.ClearValuesOverBytes {
			value = ""
		}
		w.token(jsontext.String(value))
	case types.QValueJSON:
		value := v.Val
		if opts.ClearValuesOverBytes > 0 && len(value) > opts.ClearValuesOverBytes {
			value = "{}"
		}
		// JSON columns are strings in the raw-table envelope, not nested values.
		w.token(jsontext.String(value))
	case types.QValueHStore:
		value := v.Val
		if opts.HStoreAsJSON {
			var err error
			value, err = datatypes.ParseHstore(value)
			if err != nil {
				w.err = err
				return
			}
			if opts.ClearValuesOverBytes > 0 && len(value) > opts.ClearValuesOverBytes {
				value = ""
			}
		}
		w.token(jsontext.String(value))
	case types.QValueQChar:
		w.token(jsontext.String(string(v.Val)))
	case types.QValueUUID:
		w.token(jsontext.String(v.Val.String()))
	case types.QValueBoolean:
		w.token(jsontext.Bool(v.Val))
	case types.QValueInt8:
		w.token(jsontext.Int(int64(v.Val)))
	case types.QValueInt16:
		w.token(jsontext.Int(int64(v.Val)))
	case types.QValueInt32:
		w.token(jsontext.Int(int64(v.Val)))
	case types.QValueInt64:
		w.token(jsontext.Int(v.Val))
	case types.QValueUInt8:
		w.token(jsontext.Uint(uint64(v.Val)))
	case types.QValueUInt16:
		w.token(jsontext.Uint(uint64(v.Val)))
	case types.QValueUInt32:
		w.token(jsontext.Uint(uint64(v.Val)))
	case types.QValueUInt64:
		w.token(jsontext.Uint(v.Val))
	case types.QValueFloat32:
		w.token(jsonFloat32(v.Val))
	case types.QValueFloat64:
		w.token(jsonFloat64(v.Val))
	case types.QValueNumeric:
		w.token(jsontext.String(v.Val.String()))
	case types.QValueTimestamp:
		w.token(jsontext.String(v.Val.Format("2006-01-02 15:04:05.999999")))
	case types.QValueTimestampTZ:
		w.token(jsontext.String(v.Val.Format("2006-01-02 15:04:05.999999-0700")))
	case types.QValueDate:
		w.token(jsontext.String(v.Val.Format("2006-01-02")))
	case types.QValueTime:
		w.token(jsontext.String(types.FormatExtendedTimeDuration(v.Val)))
	case types.QValueTimeTZ:
		w.token(jsontext.String(types.FormatExtendedTimeDuration(v.Val)))
	case types.QValueArrayFloat32:
		w.array(v.Val, jsonFloat32)
	case types.QValueArrayFloat64:
		w.array(v.Val, jsonFloat64)
	case types.QValueArrayNumeric:
		w.array(v.Val, func(v decimal.Decimal) jsontext.Token { return jsontext.String(v.String()) })
	case types.QValueArrayDate:
		w.array(v.Val, func(v time.Time) jsontext.Token { return jsontext.String(v.Format("2006-01-02")) })
	case types.QValueArrayTime:
		w.array(v.Val, func(v time.Duration) jsontext.Token {
			return jsontext.String(types.FormatExtendedTimeDuration(v))
		})
	default:
		// Preserve the existing Value() contract for remaining types (including
		// base64 bytes, nil slices, UUID arrays, and big integers).
		if w.err == nil {
			w.err = jsonv2.MarshalEncode(w.enc, v.Value(), json.DefaultOptionsV1())
		}
	}
}
