package exceptions

// SSHError represents errors during SSH operations, the SSH library we use does not provide a common error type, just does fmt.Errorf
type SSHError struct {
	error
}

func (e *SSHError) Error() string {
	return "SSH Error: " + e.error.Error()
}

func (e *SSHError) Unwrap() error {
	return e.error
}

func NewSSHError(err error) *SSHError {
	return &SSHError{err}
}
