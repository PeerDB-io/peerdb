package exceptions

type CloudSQLIAMAuthError struct {
	error
}

func NewCloudSQLIAMAuthError(err error) *CloudSQLIAMAuthError {
	return &CloudSQLIAMAuthError{err}
}

func (e *CloudSQLIAMAuthError) Error() string {
	return "Cloud SQL IAM Auth error: " + e.error.Error()
}

func (e *CloudSQLIAMAuthError) Unwrap() error {
	return e.error
}
