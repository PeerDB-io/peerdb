package types

import (
	"slices"
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
	return slices.EqualFunc(q.Fields, other.Fields, func(x QField, y QField) bool {
		return strings.EqualFold(x.Name, y.Name)
	})
}

// GetColumnNames returns a slice of column names.
func (q QRecordSchema) GetColumnNames() []string {
	names := make([]string, 0, len(q.Fields))
	for _, field := range q.Fields {
		names = append(names, field.Name)
	}
	return names
}

// PgxFieldDebug captures raw pgx FieldDescription data for debugging
type PgxFieldDebug struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
}

// PgAttributeDebug captures a row from pg_attribute query for debugging
type PgAttributeDebug struct {
	AttRelID    uint32
	AttNum      int16
	AttName     string
	AttNotNull  bool
	AttTypID    uint32
	AttInhCount int32
	AttIsLocal  bool
}

// TableDebug captures table metadata including parent OID for debugging
type TableDebug struct {
	OID        uint32
	TableName  string
	SchemaName string
	ParentOID  uint32 // 0 if no parent
}

// NullableSchemaDebug captures ALL raw data for debugging nullable mismatches
type NullableSchemaDebug struct {
	// All field descriptions from pgx
	PgxFields []PgxFieldDebug
	// All rows returned from pg_attribute query (for all table OIDs)
	PgAttributeRows []PgAttributeDebug
	// The table OIDs we queried
	QueriedTableOIDs []uint32
	// Per-field: would this column be nullable under strict mode?
	// Index matches qfields order. True = nullable, False = NOT nullable under strict
	StrictNullable []bool
	// Table metadata including names, schemas, and inheritance chain
	Tables []TableDebug
}
