package exceptions

type NotFoundError struct {
	error
}

func NewNotFoundError(err error) *NotFoundError {
	return &NotFoundError{error: err}
}

func (e *NotFoundError) Error() string {
	return "NotFoundError: " + e.error.Error()
}

func (e *NotFoundError) Unwrap() error {
	return e.error
}
