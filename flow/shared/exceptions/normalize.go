package exceptions

type NormalizationError struct {
	error
}

func NewNormalizationError(err error) *NormalizationError {
	return &NormalizationError{err}
}

func (e *NormalizationError) Error() string {
	return "Normalization Error: " + e.error.Error()
}

func (e *NormalizationError) Unwrap() error {
	return e.error
}
