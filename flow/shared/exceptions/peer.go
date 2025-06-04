package exceptions

type PeerCreateError struct {
	error
}

func NewPeerCreateError(err error) *PeerCreateError {
	return &PeerCreateError{err}
}

func (e *PeerCreateError) Error() string {
	return "PeerCreate Error: " + e.error.Error()
}

func (e *PeerCreateError) Unwrap() error {
	return e.error
}
