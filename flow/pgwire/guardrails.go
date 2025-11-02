package pgwire

import (
	"fmt"
	"strings"
)

// Guardrails enforces safety limits on queries
type Guardrails struct {
	MaxRows      int64
	MaxBytes     int64
	DenyList     []string
	AllowList    []string
	currentRows  int64
	currentBytes int64
}

// Default dangerous keywords that are denied unless explicitly allowed
var defaultDangerousKeywords = []string{
	"COPY",   // Protocol not supported, can execute programs with TO PROGRAM
	"DO",     // Can execute arbitrary PL/pgSQL code
	"VACUUM", // Administrative operation
	"ANALYZE", "CLUSTER", "REINDEX", // Administrative operations
	"REFRESH", // Can trigger expensive operations
	"LISTEN", "UNLISTEN", "NOTIFY", // Async messaging (may not be appropriate for proxy)
}

// NewGuardrails creates a new Guardrails instance
// Dangerous keywords are added to deny list by default unless explicitly allowed
func NewGuardrails(maxRows, maxBytes int64, denyList, allowList []string) *Guardrails {
	// Build final deny list: start with user's deny list
	finalDenyList := make([]string, 0, len(denyList)+len(defaultDangerousKeywords))
	finalDenyList = append(finalDenyList, denyList...)

	// Add default dangerous keywords unless they're explicitly allowed
	for _, dangerous := range defaultDangerousKeywords {
		// Check if explicitly allowed
		allowed := false
		for _, allowedKeyword := range allowList {
			if strings.EqualFold(dangerous, strings.TrimSpace(allowedKeyword)) {
				allowed = true
				break
			}
		}

		if !allowed {
			// Check if already in deny list
			alreadyDenied := false
			for _, denied := range denyList {
				if strings.EqualFold(dangerous, strings.TrimSpace(denied)) {
					alreadyDenied = true
					break
				}
			}

			if !alreadyDenied {
				finalDenyList = append(finalDenyList, dangerous)
			}
		}
	}

	return &Guardrails{
		MaxRows:   maxRows,
		MaxBytes:  maxBytes,
		DenyList:  finalDenyList,
		AllowList: allowList,
	}
}

// Reset resets the row and byte counters for a new query
func (g *Guardrails) Reset() {
	g.currentRows = 0
	g.currentBytes = 0
}

// CheckQuery validates a query against the deny/allow lists
// IMPORTANT: Checks each statement individually to prevent bypassing via multi-statement queries
func (g *Guardrails) CheckQuery(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil
	}

	// Split into individual statements to prevent bypassing allow/deny lists
	// e.g., "SELECT 1; DROP TABLE x;" must check both statements
	statements := splitSQL(query)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Get the first keyword from this statement
		keyword := getStatementPrefix(stmt)
		if keyword == "" {
			continue
		}

		// If allow list is set, only allow statements with allowed keywords
		if len(g.AllowList) > 0 {
			allowed := false
			for _, allowedKeyword := range g.AllowList {
				if strings.EqualFold(keyword, strings.TrimSpace(allowedKeyword)) {
					allowed = true
					break
				}
			}
			if !allowed {
				return fmt.Errorf("statement not in allow list: %s", keyword)
			}
		}

		// Check deny list
		for _, deniedKeyword := range g.DenyList {
			if strings.EqualFold(keyword, strings.TrimSpace(deniedKeyword)) {
				return fmt.Errorf("statement denied: %s", keyword)
			}
		}
	}

	return nil
}

// AddRow increments the row counter and checks the limit
func (g *Guardrails) AddRow() error {
	g.currentRows++
	if g.MaxRows > 0 && g.currentRows > g.MaxRows {
		return fmt.Errorf("row limit exceeded: %d rows (limit: %d)", g.currentRows, g.MaxRows)
	}
	return nil
}

// AddBytes increments the byte counter and checks the limit
func (g *Guardrails) AddBytes(bytes int64) error {
	g.currentBytes += bytes
	if g.MaxBytes > 0 && g.currentBytes > g.MaxBytes {
		return fmt.Errorf("byte limit exceeded: %d bytes (limit: %d)", g.currentBytes, g.MaxBytes)
	}
	return nil
}

// GetStats returns current row and byte counts
func (g *Guardrails) GetStats() (int64, int64) {
	return g.currentRows, g.currentBytes
}
