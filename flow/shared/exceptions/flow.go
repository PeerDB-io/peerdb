package exceptions

import "fmt"

type DropFlowError struct {
	error
}

func NewDropFlowError(format string, a ...any) *DropFlowError {
	return &DropFlowError{fmt.Errorf(format, a...)}
}

func (e *DropFlowError) Error() string {
	return "DropFlow Error: " + e.error.Error()
}

func (e *DropFlowError) Unwrap() error {
	return e.error
}
