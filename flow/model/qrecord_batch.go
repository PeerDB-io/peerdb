package model

type QRecordSchema struct {
	ColumnNames []string
	ColumnTypes []QValueKind
}

// NewQRecordSchema creates a new QRecordSchema.
func NewQRecordSchema(columnNames []string, columnTypes []QValueKind) *QRecordSchema {
	return &QRecordSchema{
		ColumnNames: columnNames,
		ColumnTypes: columnTypes,
	}
}

// EqualNames returns true if the column names are equal.
func (q *QRecordSchema) EqualNames(other *QRecordSchema) bool {
	if other == nil {
		return q == nil
	}

	if len(q.ColumnNames) != len(other.ColumnNames) {
		return false
	}

	for i, name := range q.ColumnNames {
		if name != other.ColumnNames[i] {
			return false
		}
	}

	return true
}

// QRecordBatch holds a batch of QRecord objects.
type QRecordBatch struct {
	NumRecords uint32     // NumRecords represents the number of records in the batch.
	Records    []*QRecord // Records is a slice of pointers to QRecord objects.
	Schema     *QRecordSchema
}

// Equals checks if two QRecordBatches are identical.
func (q *QRecordBatch) Equals(other *QRecordBatch) bool {
	if other == nil {
		return q == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords {
		return false
	}

	// Compare column names
	if !q.Schema.EqualNames(other.Schema) {
		return false
	}

	// Compare records
	for i, record := range q.Records {
		if !record.equals(other.Records[i]) {
			return false
		}
	}

	return true
}
