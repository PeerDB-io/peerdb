package exceptions

type BigQueryError struct {
	error
}

func NewBigQueryError(err error) *BigQueryError {
	return &BigQueryError{err}
}

func (e *BigQueryError) Error() string {
	return "BigQuery Error: " + e.error.Error()
}

func (e *BigQueryError) Unwrap() error {
	return e.error
}
