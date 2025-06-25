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

type NumericOutOfRangeError struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewNumericOutOfRangeError(err error, destinationTable, destinationColumn string) *NumericOutOfRangeError {
	return &NumericOutOfRangeError{err, destinationTable, destinationColumn}
}

func (e *NumericOutOfRangeError) Error() string {
	return e.error.Error()
}

func (e *NumericOutOfRangeError) Unwrap() error {
	return e.error
}
