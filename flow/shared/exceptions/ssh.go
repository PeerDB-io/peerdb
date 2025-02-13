package exceptions

// SSHTunnelSetupError represents errors during SSH Tunnel Setup, the SSH library we use does not provide a common error type,
// just does fmt.Errorf
type SSHTunnelSetupError struct {
	error
}

func (e *SSHTunnelSetupError) Error() string {
	return "SSH Tunnel Setup Error: " + e.error.Error()
}

func (e *SSHTunnelSetupError) Unwrap() error {
	return e.error
}

func NewSSHTunnelSetupError(err error) *SSHTunnelSetupError {
	return &SSHTunnelSetupError{err}
}
