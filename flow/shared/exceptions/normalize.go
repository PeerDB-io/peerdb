package exceptions

import "fmt"

type NormalizationError struct {
	error
}

func NewNormalizationError(format string, a ...any) *NormalizationError {
	return &NormalizationError{fmt.Errorf(format, a...)}
}

func (e *NormalizationError) Error() string {
	return "Normalization Error: " + e.error.Error()
}

func (e *NormalizationError) Unwrap() error {
	return e.error
}
