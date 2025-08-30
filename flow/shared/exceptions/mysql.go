package exceptions

type MySQLIncompatibleColumnTypeError struct {
	message string
}

func NewIncompatibleColumnTypeError(message string) MySQLIncompatibleColumnTypeError {
	return MySQLIncompatibleColumnTypeError{
		message: message,
	}
}

func (e MySQLIncompatibleColumnTypeError) Error() string {
	return e.message
}
