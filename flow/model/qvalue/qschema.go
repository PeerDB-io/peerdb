package qvalue

import (
	"strings"
)

type QField struct {
	Name      string
	Type      QValueKind
	Precision int16
	Scale     int16
	Nullable  bool
}

type QRecordSchema struct {
	Fields []QField
}

// NewQRecordSchema creates a new QRecordSchema.
func NewQRecordSchema(fields []QField) QRecordSchema {
	return QRecordSchema{Fields: fields}
}

// EqualNames returns true if the field names are equal.
func (q QRecordSchema) EqualNames(other QRecordSchema) bool {
	if len(q.Fields) != len(other.Fields) {
		return false
	}

	for i, field := range q.Fields {
		if !strings.EqualFold(field.Name, other.Fields[i].Name) {
			return false
		}
	}

	return true
}

// GetColumnNames returns a slice of column names.
func (q QRecordSchema) GetColumnNames() []string {
	names := make([]string, 0, len(q.Fields))
	for _, field := range q.Fields {
		names = append(names, field.Name)
	}
	return names
}
