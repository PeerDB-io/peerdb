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

// CockroachChangefeedIrrecoverableError marks changefeed failures no retry can
// fix: the cursor fell behind the replica GC threshold, or a watched table was
// truncated or dropped. The mirror needs operator action, typically a resync,
// and the alerting classifier notifies the user instead of retrying silently.
type CockroachChangefeedIrrecoverableError struct {
	error
	// Code is a stable machine-readable reason: CURSOR_PAST_GC,
	// TABLE_TRUNCATED or TABLE_DROPPED.
	Code string
}

func NewCockroachChangefeedIrrecoverableError(code string, err error) *CockroachChangefeedIrrecoverableError {
	return &CockroachChangefeedIrrecoverableError{error: err, Code: code}
}

func (e *CockroachChangefeedIrrecoverableError) Unwrap() error {
	return e.error
}
