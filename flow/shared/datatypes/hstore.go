/*
This is in reference to PostgreSQL's hstore:
https://github.com/postgres/postgres/blob/bea18b1c949145ba2ca79d4765dba3cc9494a480/contrib/hstore/hstore_io.c

This package is an implementation based on the above code.
It's simplified to only parse the subset which `hstore_out` outputs.
*/
package datatypes

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type hstore map[string]*string

type hstoreParser struct {
	str           string
	pos           int
	nextBackslash int
}

func newHSP(in string) *hstoreParser {
	return &hstoreParser{
		pos:           0,
		str:           in,
		nextBackslash: strings.IndexByte(in, '\\'),
	}
}

func (p *hstoreParser) atEnd() bool {
	return p.pos >= len(p.str)
}

// consume returns the next byte of the string, or end if the string is done.
func (p *hstoreParser) consume() (byte, bool) {
	if p.pos >= len(p.str) {
		return 0, true
	}
	b := p.str[p.pos]
	p.pos++
	return b, false
}

func unexpectedByteErr(actualB byte, expectedB byte) error {
	return fmt.Errorf("expected '%c' ('%#v'); found '%c' ('%#v')", expectedB, expectedB, actualB, actualB)
}

// consumeExpectedByte consumes expectedB from the string, or returns an error.
func (p *hstoreParser) consumeExpectedByte(expectedB byte) error {
	nextB, end := p.consume()
	if end {
		return fmt.Errorf("expected '%c' ('%#v'); found end", expectedB, expectedB)
	}
	if nextB != expectedB {
		return unexpectedByteErr(nextB, expectedB)
	}
	return nil
}

func (p *hstoreParser) consumeExpected2(one byte, two byte) error {
	if p.pos+2 > len(p.str) {
		return errors.New("unexpected end of string")
	}
	if p.str[p.pos] != one {
		return unexpectedByteErr(p.str[p.pos], one)
	}
	if p.str[p.pos+1] != two {
		return unexpectedByteErr(p.str[p.pos+1], two)
	}
	p.pos += 2
	return nil
}

var errEOSInQuoted = errors.New(`found end before closing double-quote ('"')`)

// consumeDoubleQuoted consumes a double-quoted string from p. The double quote must have been
// parsed already.
func (p *hstoreParser) consumeDoubleQuoted() (string, error) {
	// fast path: assume most keys/values do not contain escapes
	nextDoubleQuote := strings.IndexByte(p.str[p.pos:], '"')
	if nextDoubleQuote == -1 {
		return "", errEOSInQuoted
	}
	nextDoubleQuote += p.pos
	if p.nextBackslash == -1 || p.nextBackslash > nextDoubleQuote {
		s := p.str[p.pos:nextDoubleQuote]
		p.pos = nextDoubleQuote + 1
		return s, nil
	}

	s, err := p.consumeDoubleQuotedWithEscapes(p.nextBackslash)
	p.nextBackslash = strings.IndexByte(p.str[p.pos:], '\\')
	if p.nextBackslash != -1 {
		p.nextBackslash += p.pos
	}
	return s, err
}

// consumeDoubleQuotedWithEscapes consumes a double-quoted string containing escapes, starting
// at p.pos, and with the first backslash at firstBackslash. This copies the string so it can be
// garbage collected separately.
func (p *hstoreParser) consumeDoubleQuotedWithEscapes(firstBackslash int) (string, error) {
	// copy the prefix that does not contain backslashes
	var builder strings.Builder
	builder.WriteString(p.str[p.pos:firstBackslash])

	// skip to the backslash
	p.pos = firstBackslash

	// copy bytes until the end, unescaping backslashes
	for {
		nextB, end := p.consume()
		if end {
			return "", errEOSInQuoted
		} else if nextB == '"' {
			break
		} else if nextB == '\\' {
			// escape: skip the backslash and copy the char
			nextB, end = p.consume()
			if end {
				return "", errEOSInQuoted
			}
			if nextB != '\\' && nextB != '"' {
				return "", fmt.Errorf("unexpected escape in quoted string: found '%#v'", nextB)
			}
			builder.WriteByte(nextB)
		} else {
			// normal byte: copy it
			builder.WriteByte(nextB)
		}
	}
	return builder.String(), nil
}

// consumePairSeparator consumes the Hstore pair separator ", " or returns an error.
func (p *hstoreParser) consumePairSeparator() error {
	return p.consumeExpected2(',', ' ')
}

// consumeKVSeparator consumes the Hstore key/value separator "=>" or returns an error.
func (p *hstoreParser) consumeKVSeparator() error {
	return p.consumeExpected2('=', '>')
}

// consumeDoubleQuotedOrNull consumes the string or returns an error.
func (p *hstoreParser) consumeDoubleQuotedOrNull() (*string, error) {
	// peek at the next byte
	if p.atEnd() {
		return nil, errors.New("found end instead of value")
	}
	next := p.str[p.pos]
	if next == 'N' {
		// must be the exact string NULL: use consumeExpected2 twice
		if err := p.consumeExpected2('N', 'U'); err != nil {
			return nil, err
		}
		if err := p.consumeExpected2('L', 'L'); err != nil {
			return nil, err
		}
		return nil, nil
	} else if next != '"' {
		return nil, unexpectedByteErr(next, '"')
	}

	// skip the double quote
	p.pos += 1
	s, err := p.consumeDoubleQuoted()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func ParseHstore(s string) (string, error) {
	p := newHSP(s)

	// This is an over-estimate of the number of key/value pairs.
	numPairsEstimate := strings.Count(s, ">")
	result := make(hstore, numPairsEstimate)
	first := true
	for !p.atEnd() {
		if !first {
			if err := p.consumePairSeparator(); err != nil {
				return "", err
			}
		} else {
			first = false
		}

		if err := p.consumeExpectedByte('"'); err != nil {
			return "", err
		}

		key, err := p.consumeDoubleQuoted()
		if err != nil {
			return "", err
		}

		if err := p.consumeKVSeparator(); err != nil {
			return "", err
		}

		value, err := p.consumeDoubleQuotedOrNull()
		if err != nil {
			return "", err
		}
		result[key] = value
	}

	jsonBytes, err := json.Marshal(result)
	return string(jsonBytes), err
}
