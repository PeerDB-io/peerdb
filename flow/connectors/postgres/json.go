package connpostgres

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strconv"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

type relaxedNumberDecoder struct{}

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
	default:
		*anyPtr = iter.Read()
	}
}

type RelaxedNumberExtension struct {
	jsoniter.DummyExtension
}

func (extension *RelaxedNumberExtension) CreateDecoder(typ reflect2.Type) jsoniter.ValDecoder {
	if typ == reflect2.TypeOfPtr((*any)(nil)).Elem() {
		return &relaxedNumberDecoder{}
	}
	return nil
}

func createExtendedJSONUnmarshaler() jsoniter.API {
	config := jsoniter.ConfigCompatibleWithStandardLibrary
	config.RegisterExtension(&RelaxedNumberExtension{})
	return config
}

// Type to identify json that has already been marshalled, vs. a JSON token
// like a string or byte slice.
type preMarshalledJson []byte

// jsonNullLiteral is the pre-marshalled JSON null literal, used to keep a JSON
// null distinguishable from a SQL NULL.
var jsonNullLiteral = preMarshalledJson("null")

func reencodeWithRelaxedNumbers(input io.Reader, sizeHint int) (preMarshalledJson, error) {
	// We use a jsontext Encoder and Decoder to walk through the input and
	// convert any numbers that fail to parse as a float into a string instead.
	//
	// We create both the decoder and the encoder with AllowInvalidUTF8 so that
	// we do not error out on seeing an invalid UTF-8 string; instead, we escape
	// it with UTF-8 valid escape characters to allow for the ingestion into
	// ClickHouse to not fail. We also use AllowDuplicateNames to not have the
	// ingestion error out in case there are duplicate object keys; we let the
	// destination ClickHouse collapse these
	dec := jsontext.NewDecoder(
		input,
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	)
	out := new(bytes.Buffer)
	if sizeHint > 0 {
		// Grow slightly past the sizeHint to account for any whitespace added by
		// the encoder below. We want to avoid repeat allocations as much as possible.
		out.Grow(int(float64(sizeHint) * 1.5))
	}
	enc := jsontext.NewEncoder(
		out,
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	)
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

func convertWithRelaxedNumbers(input *bytes.Buffer, sizeHint int) (preMarshalledJson, error) {
	// First pass: Try to copy the input byte-for-byte into an output buffer,
	// while walking through it with a jsontext encoder that's expected to
	// reject invalid UTF8 strings. If we see an invalid utf8 string,
	// revert to reencodeWithRelaxedNumbers, which is less performant.
	//
	// We also reuse the input bytes.Buffer directly as jsontext.NewDecoder is
	// optimized for bytes.Buffer. We need to create two new readers, as the
	// decoder would advance the internal reader state of `input` itself. One
	// is for use with reencodeWithRelaxedNumbers, and another is for our own
	// manual byte copying.
	savedReaderForReencode := bytes.NewReader(input.Bytes())
	savedReader := bytes.NewReader(input.Bytes())
	inputBytes := input.Bytes()
	var readSoFar int64
	dec := jsontext.NewDecoder(
		input,
		jsontext.AllowInvalidUTF8(false),
		jsontext.AllowDuplicateNames(true),
	)
	out := new(bytes.Buffer)
	if sizeHint > 0 {
		// Grow slightly past the sizeHint to account for any whitespace added by
		// the encoder below. We want to avoid repeat allocations as much as possible.
		out.Grow(int(float64(sizeHint) * 1.5))
	}
	for {
		// Read a token from the input.
		tok, err := dec.ReadToken()
		if err != nil {
			if err == io.EOF {
				break
			}
			return reencodeWithRelaxedNumbers(savedReaderForReencode, sizeHint)
		}

		// Check if the token is a number.
		if tok.Kind() == jsontext.KindNumber {
			_, err := tok.Float()
			if err != nil {
				if !errors.Is(err, strconv.ErrRange) {
					return nil, err
				}

				// Float is out of range. Convert tok to a string.
				rawNumber := tok.String()

				end := int(dec.InputOffset() - readSoFar)
				start := end - len(rawNumber)
				if out.Available() < (end + 2) {
					out.Grow(out.Len() + end + 2)
				}
				savedReader.Read(out.AvailableBuffer()[:start])
				_, _ = out.Write(out.AvailableBuffer()[:start])
				_ = out.WriteByte('"')
				savedReader.Read(out.AvailableBuffer()[:len(rawNumber)])
				_, _ = out.Write(out.AvailableBuffer()[:len(rawNumber)])
				_ = out.WriteByte('"')
				readSoFar = dec.InputOffset()
			}
		}
	}
	if readSoFar == 0 {
		// Zero-copy fast path.
		return preMarshalledJson(inputBytes), nil
	}

	out.ReadFrom(savedReader)
	return preMarshalledJson(out.Bytes()), nil
}
