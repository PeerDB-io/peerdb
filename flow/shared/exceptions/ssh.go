package exceptions

import "strings"

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

// SSHTunnelDialError represents a failure to open a new connection through an established SSH tunnel.
type SSHTunnelDialError struct {
	error
	Retryable bool
}

func NewSSHTunnelDialError(err error) *SSHTunnelDialError {
	// x/crypto/ssh's mux returns this untyped error when the SSH connection
	// is torn down while a channel open is in flight.
	if strings.Contains(err.Error(), "unexpected packet in response to channel open") {
		return &SSHTunnelDialError{err, true}
	}
	return &SSHTunnelDialError{err, false}
}

func (e *SSHTunnelDialError) Error() string {
	return "SSH Tunnel Dial Error: " + e.error.Error()
}

func (e *SSHTunnelDialError) Unwrap() error {
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

type SSHTunnelClosedError struct {
	error
}

func NewSSHTunnelClosedError(err error) *SSHTunnelClosedError {
	return &SSHTunnelClosedError{err}
}

func (e *SSHTunnelClosedError) Error() string {
	return "SSH Tunnel Closed: " + e.error.Error()
}

func (e *SSHTunnelClosedError) Unwrap() error {
	return e.error
}
