package connpostgres

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

// relaxedNumberDecoder converts numbers that are out of float64 range into strings.
// It also tracks duplicate object keys and increments `duplicateKeys` when seen.
type relaxedNumberDecoder struct {
	duplicateKeys *atomic.Int64
}

func (d *relaxedNumberDecoder) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	anyPtr := (*any)(ptr)
	switch iter.WhatIsNext() {
	case jsoniter.NumberValue:
		numberToken := iter.ReadNumber()
		if val, err := numberToken.Float64(); err == nil {
			*anyPtr = val
		} else {
			*anyPtr = numberToken.String()
		}
	case jsoniter.ObjectValue:
		// decode objects ourselves to count duplicate keys; last occurrence wins.
		obj := make(map[string]any)
		iter.ReadMapCB(func(it *jsoniter.Iterator, field string) bool {
			var elem any
			d.Decode(unsafe.Pointer(&elem), it)
			if _, ok := obj[field]; ok {
				d.duplicateKeys.Add(1)
			}
			obj[field] = elem
			return true
		})
		*anyPtr = obj
	case jsoniter.ArrayValue:
		arr := []any{}
		iter.ReadArrayCB(func(it *jsoniter.Iterator) bool {
			var elem any
			d.Decode(unsafe.Pointer(&elem), it)
			arr = append(arr, elem)
			return true
		})
		*anyPtr = arr
	default:
		*anyPtr = iter.Read()
	}
}

type RelaxedNumberExtension struct {
	jsoniter.DummyExtension
	duplicateKeys atomic.Int64
}

func (extension *RelaxedNumberExtension) CreateDecoder(typ reflect2.Type) jsoniter.ValDecoder {
	if typ == reflect2.TypeOfPtr((*any)(nil)).Elem() {
		return &relaxedNumberDecoder{duplicateKeys: &extension.duplicateKeys}
	}
	return nil
}

func createExtendedJSONUnmarshaler() (jsoniter.API, *RelaxedNumberExtension) {
	// jsoniter.ConfigCompatibleWithStandardLibrary is shared via a global var,
	// so we make a clean copy of it to ensure that the returned marshaller only
	// has one extension registered. This is important so we correctly track
	// the correct automic inside `RelaxedNumberExtension`.
	config := jsoniter.Config{
		EscapeHTML:             true,
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
	ext := &RelaxedNumberExtension{}
	config.RegisterExtension(ext)
	return config, ext
}

// jsonNullLiteral is the pre-marshalled JSON null literal, used to keep a JSON
// null distinguishable from a SQL NULL.
var jsonNullLiteral = []byte("null")

func convertWithRelaxedNumbers(input io.Reader, sizeHint int) ([]byte, error) {
	// We use a jsontext Encoder and Decoder to walk through the input and
	// convert any numbers that fail to parse as a float into a string instead.
	//
	// We create both the decoder and the encoder with AllowInvalidUTF8 so that
	// we do not error out on seeing an invalid UTF-8 string; instead, we escape
	// it with UTF-8 valid escape characters to allow for the ingestion into
	// ClickHouse to not fail.
	dec := jsontext.NewDecoder(input, jsontext.AllowInvalidUTF8(true), jsontext.Multiline(false))
	out := new(bytes.Buffer)
	if sizeHint > 0 {
		// Grow slightly past the sizeHint to account for any whitespace added by
		// the encoder below. We want to avoid repeat allocations as much as possible.
		out.Grow(int(float64(sizeHint) * 1.5))
	}
	enc := jsontext.NewEncoder(out, jsontext.AllowInvalidUTF8(true), jsontext.Multiline(false))
	for {
		// Read a token from the input.
		tok, err := dec.ReadToken()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Check if the token is a number.
		if tok.Kind() == jsontext.KindNumber {
			_, err := tok.Float()
			if err != nil {
				if !errors.Is(err, strconv.ErrRange) {
					return nil, err
				}
				// Float is out of range. Convert tok to a string.
				tok = jsontext.String(tok.String())
			}
		}

		if err := enc.WriteToken(tok); err != nil {
			return nil, err
		}
	}
	// The encoder terminates each top-level value with a newline; callers treat
	// the result as a single JSON value, so trim it.
	return bytes.TrimRight(out.Bytes(), "\n"), nil
}
