package clickhouse

import (
	"fmt"
	"slices"
	"strings"
)

// InvalidFragmentError reports user-provided SQL, such as a column type, that can't be put in a query as is.
type InvalidFragmentError struct {
	Kind     string
	Fragment string
	Reason   string
}

func (e *InvalidFragmentError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Kind, e.Fragment, e.Reason)
}

// ValidateColumnType accepts a single type built from names, parentheses, commas and integers, like Decimal(38,10).
// Whitespace is rejected, as other code compares types as strings.
func ValidateColumnType(columnType string) error {
	p := typeParser{input: columnType}
	err := p.parseType()
	if err == nil {
		if p.pos < len(p.input) {
			err = p.unexpected("end of type")
		}
	}
	if err != nil {
		return &InvalidFragmentError{Kind: "column type", Fragment: columnType, Reason: err.Error()}
	}
	return nil
}

type typeParser struct {
	input string
	pos   int
}

// columnDeclarationKeywords are read as column options in place of a type, like DEFAULT(expression).
var columnDeclarationKeywords = []string{
	"ALIAS", "AUTO_INCREMENT", "CODEC", "COMMENT", "CONSTRAINT", "DEFAULT", "EPHEMERAL", "MATERIALIZED",
	"NOT", "NULL", "PRIMARY", "SETTINGS", "STATISTICS", "TTL",
}

// parseType parses `type := name [ '(' arg { ',' arg } ')' ]`, where `arg := type | integer`.
func (p *typeParser) parseType() error {
	start := p.pos
	if !p.consumeWhile(isIdentStart, isIdentChar) {
		return p.unexpected("a type name")
	}
	name := p.input[start:p.pos]
	if slices.ContainsFunc(columnDeclarationKeywords, func(keyword string) bool { return strings.EqualFold(name, keyword) }) {
		return fmt.Errorf("%s at offset %d is a keyword, not a type", name, start)
	}
	if !p.consumeByte('(') {
		return nil
	}
	for {
		if !p.consumeWhile(isDigit, isDigit) {
			if err := p.parseType(); err != nil {
				return err
			}
		}
		if p.consumeByte(')') {
			return nil
		}
		if !p.consumeByte(',') {
			return p.unexpected(`"," or ")"`)
		}
	}
}

func (p *typeParser) consumeByte(c byte) bool {
	if p.pos < len(p.input) && p.input[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

func (p *typeParser) consumeWhile(first func(byte) bool, rest func(byte) bool) bool {
	if p.pos >= len(p.input) || !first(p.input[p.pos]) {
		return false
	}
	p.pos++
	for p.pos < len(p.input) && rest(p.input[p.pos]) {
		p.pos++
	}
	return true
}

func (p *typeParser) unexpected(expected string) error {
	if p.pos >= len(p.input) {
		return fmt.Errorf("expected %s at the end", expected)
	}
	return fmt.Errorf("expected %s at offset %d, found %q", expected, p.pos, p.input[p.pos])
}

// ValidateExpression accepts an expression, like a PARTITION BY key, that can't escape the parentheses it's put in.
// It allows names, numbers, arithmetic, commas and parentheses, but no strings, comments, subqueries or aliases.
func ValidateExpression(expression string) error {
	reject := func(reason string, args ...any) error {
		return &InvalidFragmentError{Kind: "expression", Fragment: expression, Reason: fmt.Sprintf(reason, args...)}
	}
	depth := 0
	for i := 0; i < len(expression); {
		c := expression[i]
		switch {
		case c == '`' || c == '"':
			end := strings.IndexByte(expression[i+1:], c)
			if end == -1 {
				return reject("unterminated quoted name at offset %d", i)
			}
			if strings.ContainsRune(expression[i+1:i+1+end], '\\') {
				return reject("backslash in quoted name at offset %d", i)
			}
			i += end + 2
		case isIdentStart(c):
			start := i
			for i < len(expression) && isIdentChar(expression[i]) {
				i++
			}
			switch strings.ToUpper(expression[start:i]) {
			case "SELECT", "WITH":
				return reject("subqueries are not supported")
			case "AS":
				return reject("aliases are not supported")
			}
		case isDigit(c):
			for i < len(expression) && (isIdentChar(expression[i]) || expression[i] == '.') {
				i++
			}
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			if depth < 0 {
				return reject("unbalanced parentheses at offset %d", i)
			}
			i++
		case strings.HasPrefix(expression[i:], "--") || strings.HasPrefix(expression[i:], "/*"):
			return reject("comments are not supported")
		case isSpace(c) || strings.IndexByte("+-*/%,", c) != -1:
			i++
		default:
			return reject("unsupported character %q at offset %d", c, i)
		}
	}
	if depth != 0 {
		return reject("unbalanced parentheses")
	}
	return nil
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || isDigit(c)
}
