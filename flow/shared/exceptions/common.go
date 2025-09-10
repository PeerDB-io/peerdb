package exceptions

type ErrNotFound struct {
	error
}

func NewErrNotFound(err error) *ErrNotFound {
	return &ErrNotFound{error: err}
}

func (e *ErrNotFound) Error() string {
	return "ErrNotFound: " + e.error.Error()
}

func (e *ErrNotFound) Unwrap() error {
	return e.error
}
