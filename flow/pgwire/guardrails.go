package pgwire

import (
	"errors"
	"fmt"
	"strings"
)

// Guardrails enforces safety limits on queries
type Guardrails struct {
	MaxRows      int64
	MaxBytes     int64
	currentRows  int64
	currentBytes int64
}

// blockedCommands are statements that are always denied
var blockedCommands = []string{
	"COPY",     // Protocol not supported + TO PROGRAM security risk
	"VACUUM",   // Maintenance op, I/O impact, VACUUM FULL rewrites tables
	"ANALYZE",  // Writes to system catalogs
	"CLUSTER",  // Rewrites entire tables
	"REINDEX",  // Rebuilds indexes, can lock tables
	"REFRESH",  // REFRESH MATERIALIZED VIEW modifies stored data
	"LISTEN",   // Async messaging not supported by proxy
	"NOTIFY",   // Async messaging not supported by proxy
	"UNLISTEN", // Async messaging not supported by proxy
	"DO",       // Anonymous PL/pgSQL blocks, can execute dynamic SQL
	"LOCK",     // Can lock tables, potential for blocking/deadlocks
}

// NewGuardrails creates a new Guardrails instance
func NewGuardrails(maxRows, maxBytes int64) *Guardrails {
	return &Guardrails{
		MaxRows:  maxRows,
		MaxBytes: maxBytes,
	}
}

// Reset resets the row and byte counters for a new query
func (g *Guardrails) Reset() {
	g.currentRows = 0
	g.currentBytes = 0
}

// CheckQuery validates a query for security
func (g *Guardrails) CheckQuery(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil
	}

	// Check for read-only bypass attempts
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "default_transaction_read_only") {
		return errors.New("cannot modify read-only mode")
	}
	if strings.Contains(lower, "set_config") {
		return errors.New("set_config is not allowed")
	}
	// Block READ WRITE transactions (BEGIN/START TRANSACTION/SET TRANSACTION READ WRITE)
	if strings.Contains(lower, "read write") {
		return errors.New("READ WRITE transactions not allowed")
	}

	// Check each statement against blocked commands
	statements := splitSQL(query)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		keyword := getStatementPrefix(stmt)
		for _, blocked := range blockedCommands {
			if strings.EqualFold(keyword, blocked) {
				return fmt.Errorf("statement denied: %s", blocked)
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
