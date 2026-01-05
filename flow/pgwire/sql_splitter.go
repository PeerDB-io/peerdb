package pgwire

import (
	"strings"
	"unicode"
)

// splitSQL splits a SQL string into individual statements, respecting quotes and comments
// This is a minimal splitter that handles:
// - Single-line comments (--)
// - Multi-line comments (/* */)
// - Single-quoted strings ('...')
// - Double-quoted identifiers ("...")
// - Dollar-quoted strings ($$...$$, $tag$...$tag$)
// - Semicolon statement separators
func splitSQL(sql string) []string {
	var statements []string
	var current strings.Builder

	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Check for single-line comment
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			// Skip until end of line
			current.WriteByte(ch)
			i++
			for i < len(sql) && sql[i] != '\n' {
				current.WriteByte(sql[i])
				i++
			}
			if i < len(sql) {
				current.WriteByte(sql[i]) // include \n
				i++
			}
			continue
		}

		// Check for multi-line comment
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			current.WriteString("/*")
			i += 2
			// Skip until */
			for i < len(sql)-1 {
				if sql[i] == '*' && sql[i+1] == '/' {
					current.WriteString("*/")
					i += 2
					break
				}
				current.WriteByte(sql[i])
				i++
			}
			continue
		}

		// Check for single-quoted string
		if ch == '\'' {
			current.WriteByte(ch)
			i++
			// Read until closing quote, handling escapes
			for i < len(sql) {
				if sql[i] == '\'' {
					current.WriteByte(sql[i])
					i++
					// Check for doubled quote (SQL escape)
					if i < len(sql) && sql[i] == '\'' {
						current.WriteByte(sql[i])
						i++
						continue
					}
					break
				}
				if sql[i] == '\\' && i+1 < len(sql) {
					// Handle backslash escape
					current.WriteByte(sql[i])
					i++
					if i < len(sql) {
						current.WriteByte(sql[i])
						i++
					}
					continue
				}
				current.WriteByte(sql[i])
				i++
			}
			continue
		}

		// Check for double-quoted identifier
		if ch == '"' {
			current.WriteByte(ch)
			i++
			// Read until closing quote
			for i < len(sql) {
				if sql[i] == '"' {
					current.WriteByte(sql[i])
					i++
					// Check for doubled quote (SQL escape)
					if i < len(sql) && sql[i] == '"' {
						current.WriteByte(sql[i])
						i++
						continue
					}
					break
				}
				current.WriteByte(sql[i])
				i++
			}
			continue
		}

		// Check for dollar-quoted string
		if ch == '$' {
			// Find the tag
			tagStart := i
			i++
			for i < len(sql) && (unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i])) || sql[i] == '_') {
				i++
			}
			if i < len(sql) && sql[i] == '$' {
				// Found opening tag
				i++
				tag := sql[tagStart:i]
				current.WriteString(tag)

				// Find closing tag
				for i < len(sql) {
					if sql[i] == '$' {
						closeStart := i
						closeEnd := i + 1
						// Try to match tag
						for closeEnd-closeStart < len(tag) && closeEnd < len(sql) {
							if sql[closeEnd] != tag[closeEnd-closeStart] {
								break
							}
							closeEnd++
						}
						if closeEnd-closeStart == len(tag) {
							// Found matching closing tag
							current.WriteString(tag)
							i = closeEnd
							break
						}
					}
					current.WriteByte(sql[i])
					i++
				}
				continue
			} else {
				// Not a dollar quote, backtrack
				current.WriteString(sql[tagStart:i])
				continue
			}
		}

		// Check for semicolon (statement separator)
		if ch == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			i++
			continue
		}

		// Regular character
		current.WriteByte(ch)
		i++
	}

	// Add final statement if any
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// getStatementPrefix returns the first SQL keyword from a statement
func getStatementPrefix(stmt string) string {
	stmt = strings.TrimSpace(stmt)

	// Skip leading comments
	for {
		if strings.HasPrefix(stmt, "--") {
			// Skip to end of line
			if idx := strings.Index(stmt, "\n"); idx >= 0 {
				stmt = strings.TrimSpace(stmt[idx+1:])
				continue
			}
			return "" // comment-only statement
		}
		if strings.HasPrefix(stmt, "/*") {
			// Skip to */
			if idx := strings.Index(stmt, "*/"); idx >= 0 {
				stmt = strings.TrimSpace(stmt[idx+2:])
				continue
			}
			return "" // unclosed comment
		}
		break
	}

	// Extract first word
	i := len(stmt)
	for j := range len(stmt) {
		ch := stmt[j]
		if !unicode.IsLetter(rune(ch)) && !unicode.IsDigit(rune(ch)) && ch != '_' {
			i = j
			break
		}
	}

	if i == 0 {
		return ""
	}

	return strings.ToUpper(stmt[:i])
}
