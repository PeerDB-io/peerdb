package model

// QRecordBatch holds a batch of QRecord objects.
type QRecordBatch struct {
	NumRecords  uint32     // NumRecords represents the number of records in the batch.
	Records     []*QRecord // Records is a slice of pointers to QRecord objects.
	ColumnNames []string   // ColumnNames is a slice of column names.
}

// Equals checks if two QRecordBatches are identical.
func (q *QRecordBatch) Equals(other *QRecordBatch) bool {
	if other == nil {
		return q == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords || len(q.ColumnNames) != len(other.ColumnNames) {
		return false
	}

	// Compare column names
	for i, colName := range q.ColumnNames {
		if colName != other.ColumnNames[i] {
			return false
		}
	}

	// Compare records
	for i, record := range q.Records {
		if !record.equals(other.Records[i]) {
			return false
		}
	}

	return true
}
