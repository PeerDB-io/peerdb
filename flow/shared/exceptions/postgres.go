package exceptions

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

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

type CatalogError struct {
	error
}

func (e *CatalogError) Unwrap() error {
	return e.error
}

func NewCatalogError(err error) *CatalogError {
	return &CatalogError{err}
}

type PostgresWalError struct {
	error
	Msg *pgproto3.ErrorResponse
}

func (e *PostgresWalError) Unwrap() error {
	return e.error
}

func (e *PostgresWalError) Error() string {
	return fmt.Sprintf("Postgres WAL error: %s, message: %+v", e.error.Error(), e.Msg)
}

func NewPostgresWalError(err error, msg *pgproto3.ErrorResponse) *PostgresWalError {
	return &PostgresWalError{err, msg}
}
