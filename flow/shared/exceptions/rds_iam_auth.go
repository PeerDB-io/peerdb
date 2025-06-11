package exceptions

type RDSIAMAuthError struct {
	error
}

func NewRDSIAMAuthError(err error) *RDSIAMAuthError {
	return &RDSIAMAuthError{err}
}

func (e *RDSIAMAuthError) Error() string {
	return "RDS IAM Auth error: " + e.error.Error()
}

func (e *RDSIAMAuthError) Unwrap() error {
	return e.error
}
