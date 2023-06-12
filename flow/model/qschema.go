package model

import "strings"

type QField struct {
	Name     string
	Type     QValueKind
	Nullable bool
}

type QRecordSchema struct {
	Fields []*QField
}

// NewQRecordSchema creates a new QRecordSchema.
func NewQRecordSchema(fields []*QField) *QRecordSchema {
	return &QRecordSchema{
		Fields: fields,
	}
}

// EqualNames returns true if the field names are equal.
func (q *QRecordSchema) EqualNames(other *QRecordSchema) bool {
	if other == nil {
		return q == nil
	}

	if len(q.Fields) != len(other.Fields) {
		return false
	}

	for i, field := range q.Fields {
		// ignore the case of the field name convert to lower case
		f1 := strings.ToLower(field.Name)
		f2 := strings.ToLower(other.Fields[i].Name)
		if f1 != f2 {
			return false
		}
	}

	return true
}

// GetColumnNames returns a slice of column names.
func (q *QRecordSchema) GetColumnNames() []string {
	var names []string
	for _, field := range q.Fields {
		names = append(names, field.Name)
	}
	return names
}
