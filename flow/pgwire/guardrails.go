package pgwire

import (
	"fmt"
)

// Guardrails enforces safety limits on query results (row and byte counts)
// Query validation (blocked commands, security checks) is handled by Upstream.CheckQuery()
type Guardrails struct {
	MaxRows      int64
	MaxBytes     int64
	currentRows  int64
	currentBytes int64
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
