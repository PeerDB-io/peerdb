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
// For WITH statements, it returns the actual DML/DDL keyword (e.g., "WITH...DELETE" returns "DELETE")
func getStatementPrefix(stmt string) string {
	// Skip leading whitespace and comments
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return ""
	}

	// Skip leading comments
	for {
		if strings.HasPrefix(stmt, "--") {
			// Skip to end of line
			if idx := strings.Index(stmt, "\n"); idx >= 0 {
				stmt = strings.TrimSpace(stmt[idx+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(stmt, "/*") {
			// Skip to */
			if idx := strings.Index(stmt, "*/"); idx >= 0 {
				stmt = strings.TrimSpace(stmt[idx+2:])
				continue
			}
			return ""
		}
		break
	}

	// Extract first word
	firstKeyword := extractKeyword(stmt)
	if firstKeyword == "" {
		return ""
	}

	// Special handling for WITH statements - need to find the actual DML/DDL keyword
	if firstKeyword == "WITH" {
		// Try to find the actual action keyword after the CTE
		// This is approximate: we look for keywords after a closing paren at depth 0
		actionKeyword := extractActionFromWith(stmt)
		if actionKeyword != "" {
			return actionKeyword
		}
		// Fall back to WITH if we can't determine the action
		return "WITH"
	}

	return firstKeyword
}

// extractKeyword extracts the first SQL keyword from a string, skipping whitespace and comments
func extractKeyword(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Skip any leading comments
	for {
		if strings.HasPrefix(s, "--") {
			if idx := strings.Index(s, "\n"); idx >= 0 {
				s = strings.TrimSpace(s[idx+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = strings.TrimSpace(s[idx+2:])
				continue
			}
			return ""
		}
		break
	}

	// Extract word
	var i int
	for i = 0; i < len(s); i++ {
		ch := s[i]
		if !unicode.IsLetter(rune(ch)) && !unicode.IsDigit(rune(ch)) && ch != '_' {
			break
		}
	}

	if i == 0 {
		return ""
	}

	return strings.ToUpper(s[:i])
}

// extractActionFromWith attempts to find the DML/DDL keyword in a WITH statement
// e.g., "WITH cte AS (...) DELETE FROM t" should return "DELETE"
func extractActionFromWith(stmt string) string {
	// Simple heuristic: scan for keywords after we've seen a complete CTE definition
	// We track paren depth and look for keywords at depth 0 after the CTE

	stmt = strings.ToUpper(stmt)
	depth := 0
	inString := false
	inDollarQuote := false
	i := 0

	// Skip past "WITH"
	for i < len(stmt) && unicode.IsSpace(rune(stmt[i])) {
		i++
	}
	if i+4 <= len(stmt) && stmt[i:i+4] == "WITH" {
		i += 4
	}

	// Scan through the statement tracking paren depth
	var currentWord strings.Builder
	sawCTEParen := false

	for i < len(stmt) {
		ch := stmt[i]

		// Track string literals (simplified - doesn't handle all cases)
		if ch == '\'' && !inDollarQuote {
			inString = !inString
			i++
			continue
		}
		if ch == '$' && !inString {
			// Simplified dollar quote detection
			inDollarQuote = !inDollarQuote
			i++
			continue
		}

		if !inString && !inDollarQuote {
			if ch == '(' {
				depth++
				sawCTEParen = true
				currentWord.Reset()
				i++
				continue
			}
			if ch == ')' {
				depth--
				currentWord.Reset()
				i++
				continue
			}

			// At depth 0 after seeing a CTE paren, look for keywords
			if depth == 0 && sawCTEParen {
				if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
					currentWord.WriteByte(ch)
				} else {
					word := currentWord.String()
					if word != "" && isActionKeyword(word) {
						return word
					}
					currentWord.Reset()
				}
			}
		}

		i++
	}

	// Check final word
	word := currentWord.String()
	if word != "" && isActionKeyword(word) {
		return word
	}

	return ""
}

// isActionKeyword returns true if the keyword is a DML/DDL action keyword
func isActionKeyword(keyword string) bool {
	switch keyword {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE",
		"MERGE", "COPY":
		return true
	}
	return false
}
