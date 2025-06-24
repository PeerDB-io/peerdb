package exceptions

type NumericTruncatedWarning struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewNumericTruncatedWarning(err error, destinationTable, destinationColumn string) *NumericTruncatedWarning {
	return &NumericTruncatedWarning{err, destinationTable, destinationColumn}
}

func (e *NumericTruncatedWarning) Error() string {
	return e.error.Error()
}

func (e *NumericTruncatedWarning) Unwrap() error {
	return e.error
}

type NumericClearedWarning struct {
	error
	DestinationTable  string
	DestinationColumn string
}

func NewNumericClearedWarning(err error, destinationTable, destinationColumn string) *NumericClearedWarning {
	return &NumericClearedWarning{err, destinationTable, destinationColumn}
}

func (e *NumericClearedWarning) Error() string {
	return e.error.Error()
}

func (e *NumericClearedWarning) Unwrap() error {
	return e.error
}
