package exceptions

type SkipLogFlowError struct {
	error
}

func NewSkipLogFlowError(err error) *SkipLogFlowError {
	return &SkipLogFlowError{err}
}

func (e *SkipLogFlowError) Error() string {
	return "Skip Log Flow Error: " + e.error.Error()
}

func (e *SkipLogFlowError) Unwrap() error {
	return e.error
}
