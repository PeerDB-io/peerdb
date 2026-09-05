package connpostgres

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
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

// preMarshalledJson is a syntactically valid JSON document ready to send to a
// destination. Keeping it distinct prevents parseJSON from encoding it as a
// Go string.
type preMarshalledJson string

// jsonNullLiteral is the pre-marshalled JSON null literal, used to keep a JSON
// null distinguishable from a SQL NULL.
const jsonNullLiteral preMarshalledJson = "null"

// convertWithRelaxedNumbers preserves the source JSON byte-for-byte except for
// numbers outside the float64 range, which it quotes. Strict UTF-8 validation
// runs on the fast path. Inputs containing invalid UTF-8 or unpaired surrogate
// escapes fall back to a duplicate-preserving re-encode that repairs them with
// the Unicode replacement character.
func convertWithRelaxedNumbers(input string) (preMarshalledJson, error) {
	converted, err := copyWithRelaxedNumbers(input)
	if err == nil {
		return converted, nil
	}

	repaired, repairErr := reencodeWithRelaxedNumbers(input)
	if repairErr != nil {
		return "", repairErr
	}
	return repaired, nil
}

func copyWithRelaxedNumbers(input string) (preMarshalledJson, error) {
	return copyWithRelaxedNumbersUnicode(input, false)
}

func copyWithRelaxedNumbersUnicode(input string, allowInvalidUTF8 bool) (preMarshalledJson, error) {
	dec := jsontext.NewDecoder(
		strings.NewReader(input),
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(allowInvalidUTF8),
	)

	var output strings.Builder
	hasSubstitution := false
	lastCopied := 0
	topLevelValues := 0
	for {
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("invalid PostgreSQL JSON: %w", err)
		}

		if tok.Kind() == jsontext.KindNumber {
			if _, err := tok.Float(); err != nil {
				if !errors.Is(err, strconv.ErrRange) {
					return "", fmt.Errorf("parse PostgreSQL JSON number: %w", err)
				}

				rawNumber := tok.String()
				end := int(dec.InputOffset())
				start := end - len(rawNumber)
				if !hasSubstitution {
					output.Grow(len(input) + 2)
					hasSubstitution = true
				}
				_, _ = output.WriteString(input[lastCopied:start])
				_ = output.WriteByte('"')
				_, _ = output.WriteString(input[start:end])
				_ = output.WriteByte('"')
				lastCopied = end
			}
		}

		if dec.StackDepth() == 0 {
			topLevelValues++
			if topLevelValues > 1 {
				return "", errors.New("invalid PostgreSQL JSON: multiple top-level values")
			}
		}
	}
	if topLevelValues == 0 {
		return "", errors.New("invalid PostgreSQL JSON: empty input")
	}
	if !hasSubstitution {
		return preMarshalledJson(input), nil
	}
	_, _ = output.WriteString(input[lastCopied:])
	return preMarshalledJson(output.String()), nil
}

// reencodeWithRelaxedNumbers is both the invalid-Unicode repair path and the
// retained benchmark reference for the previous token decode/encode approach.
func reencodeWithRelaxedNumbers(input string) (preMarshalledJson, error) {
	// We use a jsontext Encoder and Decoder to walk through the input and
	// convert any numbers that fail to parse as a float into a string instead.
	dec := jsontext.NewDecoder(
		strings.NewReader(input),
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
		jsontext.Multiline(false),
	)
	var out strings.Builder
	out.Grow(int(float64(len(input)) * 1.5))
	enc := jsontext.NewEncoder(&out,
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
		jsontext.Multiline(false),
	)
	topLevelValues := 0
	for {
		// Read a token from the input.
		tok, err := dec.ReadToken()
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		// Check if the token is a number.
		if tok.Kind() == jsontext.KindNumber {
			_, err := tok.Float()
			if err != nil {
				if !errors.Is(err, strconv.ErrRange) {
					return "", err
				}
				// Float is out of range. Convert tok to a string.
				tok = jsontext.String(tok.String())
			}
		}

		if err := enc.WriteToken(tok); err != nil {
			return "", err
		}
		if dec.StackDepth() == 0 {
			topLevelValues++
			if topLevelValues > 1 {
				return "", errors.New("invalid PostgreSQL JSON: multiple top-level values")
			}
		}
	}
	if topLevelValues == 0 {
		return "", errors.New("invalid PostgreSQL JSON: empty input")
	}
	// The encoder terminates each top-level value with a newline; callers treat
	// the result as a single JSON value, so trim it.
	return preMarshalledJson(strings.TrimRight(out.String(), "\n")), nil
}
