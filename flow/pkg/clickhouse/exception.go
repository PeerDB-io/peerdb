package clickhouse

type ViewError struct {
	error
}

func NewViewError(err error) *ViewError {
	return &ViewError{err}
}

func (e *ViewError) Error() string {
	return "ClickHouse view error: " + e.error.Error()
}

func (e *ViewError) Unwrap() error {
	return e.error
}
