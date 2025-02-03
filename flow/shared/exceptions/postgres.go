package exceptions

// PostgresSetupError represents errors during setup of Postgres peers, maybe we can later replace with a more generic error type
type PostgresSetupError struct {
	error
}

func (e *PostgresSetupError) Error() string {
	return "Postgres setup error: " + e.error.Error()
}

func (e *PostgresSetupError) Unwrap() error {
	return e.error
}

func NewPostgresSetupError(err error) *PostgresSetupError {
	return &PostgresSetupError{err}
}
