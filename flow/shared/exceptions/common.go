package exceptions

type NotFoundError struct {
	error
}

func NewNotFoundError(err error) *NotFoundError {
	return &NotFoundError{error: err}
}

func (e *NotFoundError) Error() string {
	return "NotFoundError: " + e.error.Error()
}

func (e *NotFoundError) Unwrap() error {
	return e.error
}

type AuthError struct {
	error
}

func NewAuthError(err error) *AuthError {
	return &AuthError{err}
}

func (e *AuthError) Error() string {
	return "auth error: " + e.error.Error()
}

func (e *AuthError) Unwrap() error {
	return e.error
}
