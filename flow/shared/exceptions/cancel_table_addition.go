package exceptions

type TableRemovalInCancellationError struct {
	error
}

func NewRemovedTablesInCancellationError(err error) *TableRemovalInCancellationError {
	return &TableRemovalInCancellationError{err}
}

func (e *TableRemovalInCancellationError) Error() string {
	return "Table Removal In Cancellation Error: " + e.error.Error()
}

func (e *TableRemovalInCancellationError) Unwrap() error {
	return e.error
}
