package model

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
