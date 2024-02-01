package model

import (
	"strings"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type QField struct {
	Name      string
	Type      qvalue.QValueKind
	Precision int16
	Scale     int16
	Nullable  bool
}

type QRecordSchema struct {
	Fields []QField
}

// NewQRecordSchema creates a new QRecordSchema.
func NewQRecordSchema(fields []QField) *QRecordSchema {
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
	names := make([]string, 0, len(q.Fields))
	for _, field := range q.Fields {
		names = append(names, field.Name)
	}
	return names
}
