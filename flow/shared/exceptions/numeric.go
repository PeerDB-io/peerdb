package exceptions

type NumericTruncatedError struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewNumericTruncatedError(err error, destinationTable, destinationColumn string) *NumericTruncatedError {
	return &NumericTruncatedError{err, destinationTable, destinationColumn}
}

func (e *NumericTruncatedError) Error() string {
	return e.error.Error()
}

func (e *NumericTruncatedError) Unwrap() error {
	return e.error
}

type NumericClearedError struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewNumericClearedError(err error, destinationTable, destinationColumn string) *NumericClearedError {
	return &NumericClearedError{err, destinationTable, destinationColumn}
}

func (e *NumericClearedError) Error() string {
	return e.error.Error()
}

func (e *NumericClearedError) Unwrap() error {
	return e.error
}
