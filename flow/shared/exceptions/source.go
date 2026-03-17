package exceptions

type ConnectionToSourceError struct {
	error
}

func NewConnectionToSourceError(err error) *ConnectionToSourceError {
	return &ConnectionToSourceError{error: err}
}

func (e *ConnectionToSourceError) Error() string {
	return "error connecting to source: " + e.error.Error()
}

func (e *ConnectionToSourceError) Unwrap() error {
	return e.error
}
