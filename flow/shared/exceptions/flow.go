package exceptions

type DropFlowError struct {
	error
}

func NewDropFlowError(err error) *DropFlowError {
	return &DropFlowError{err}
}

func (e *DropFlowError) Error() string {
	return "DropFlow Error: " + e.error.Error()
}

func (e *DropFlowError) Unwrap() error {
	return e.error
}
