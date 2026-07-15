package exceptions

type GCSError struct {
	error
}

func NewGCSError(err error) *GCSError {
	return &GCSError{err}
}

func (e *GCSError) Error() string {
	return "GCS Error: " + e.error.Error()
}

func (e *GCSError) Unwrap() error {
	return e.error
}
