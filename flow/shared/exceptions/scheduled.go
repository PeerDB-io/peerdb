package exceptions

type RecordMetricsError struct {
	error
}

func NewRecordMetricsError(err error) *RecordMetricsError {
	return &RecordMetricsError{error: err}
}

func (e *RecordMetricsError) Error() string {
	return "RecordMetricsError: " + e.error.Error()
}

func (e *RecordMetricsError) Unwrap() error {
	return e.error
}
