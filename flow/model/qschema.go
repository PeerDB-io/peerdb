package model

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
		if field.Name != other.Fields[i].Name {
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
