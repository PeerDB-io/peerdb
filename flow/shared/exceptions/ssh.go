package exceptions

// SSHTunnelSetupError represents errors during SSH Tunnel Setup, the SSH library we use does not provide a common error type,
// just does fmt.Errorf
type SSHTunnelSetupError struct {
	error
}

func NewSSHTunnelSetupError(err error) *SSHTunnelSetupError {
	return &SSHTunnelSetupError{err}
}

func (e *SSHTunnelSetupError) Error() string {
	return "SSH Tunnel Setup Error: " + e.error.Error()
}

func (e *SSHTunnelSetupError) Unwrap() error {
	return e.error
}

// SSHTunnelConnectionError represents errors that surface on a connection tunneled over SSH after the
// SSH tunnel has gone bad (e.g. keepalive failure causes us to close the underlying connections mid-CDC).
type SSHTunnelConnectionError struct {
	error
}

func NewSSHTunnelConnectionError(err error) *SSHTunnelConnectionError {
	return &SSHTunnelConnectionError{err}
}

func (e *SSHTunnelConnectionError) Error() string {
	return "SSH Tunnel Connection Error: " + e.error.Error()
}

func (e *SSHTunnelConnectionError) Unwrap() error {
	return e.error
}
