package exceptions

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgresSetupError represents errors during setup of Postgres peers, maybe we can later replace with a more generic error type
type PostgresSetupError struct {
	error
}

func NewPostgresSetupError(err error) *PostgresSetupError {
	return &PostgresSetupError{err}
}

func (e *PostgresSetupError) Error() string {
	return "Postgres setup error: " + e.error.Error()
}

func (e *PostgresSetupError) Unwrap() error {
	return e.error
}

type CatalogError struct {
	error
}

func NewCatalogError(err error) *CatalogError {
	return &CatalogError{err}
}

func (e *CatalogError) Unwrap() error {
	return e.error
}

type PostgresWalError struct {
	error
	Msg *pgproto3.ErrorResponse
}

func NewPostgresWalError(err error, msg *pgproto3.ErrorResponse) *PostgresWalError {
	return &PostgresWalError{err, msg}
}

func (e *PostgresWalError) Unwrap() error {
	return e.error
}

func (e *PostgresWalError) Error() string {
	return fmt.Sprintf("Postgres WAL error: %s, message: %+v", e.error.Error(), e.Msg)
}

func (e *PostgresWalError) UnderlyingError() *pgproto3.ErrorResponse {
	return e.Msg
}

type TablesNotInPublicationError struct {
	tables          []string
	publicationName string
}

func NewTablesNotInPublicationError(tables []string, publicationName string) *TablesNotInPublicationError {
	return &TablesNotInPublicationError{tables, publicationName}
}

func (e *TablesNotInPublicationError) Error() string {
	return fmt.Sprintf("some additional tables not present in user-provided publication %s: %s",
		e.publicationName,
		strings.Join(e.tables, ","))
}
