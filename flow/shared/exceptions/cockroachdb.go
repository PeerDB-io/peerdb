package exceptions

// CockroachDBError wraps errors returned by CockroachDB connections. CockroachDB
// speaks the Postgres wire protocol and reuses its SQLSTATE codes, so without
// this wrapper error classification would attribute CockroachDB errors to Postgres.
type CockroachDBError struct {
	error
}

func NewCockroachDBError(err error) *CockroachDBError {
	return &CockroachDBError{err}
}

func (e *CockroachDBError) Unwrap() error {
	return e.error
}
