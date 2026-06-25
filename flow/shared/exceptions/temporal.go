package exceptions

type TemporalClampedError struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewTemporalClampedError(err error, destinationTable, destinationColumn string) *TemporalClampedError {
	return &TemporalClampedError{err, destinationTable, destinationColumn}
}

func (e *TemporalClampedError) Error() string {
	return e.error.Error()
}

func (e *TemporalClampedError) Unwrap() error {
	return e.error
}
