package pgwire

import (
	"errors"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// writeErrorResponse converts an error to a PostgreSQL ErrorResponse and writes it to the connection
// If timeout is 0, uses the default from env var
func writeErrorResponse(w io.Writer, err error, timeout time.Duration) error {
	var pgErr *pgconn.PgError
	var er *pgproto3.ErrorResponse

	if errors.As(err, &pgErr) {
		// Convert pgconn.PgError to ErrorResponse with all fields
		er = &pgproto3.ErrorResponse{
			Severity:         pgErr.Severity,
			Code:             pgErr.Code,
			Message:          pgErr.Message,
			Detail:           pgErr.Detail,
			Hint:             pgErr.Hint,
			Position:         pgErr.Position,
			InternalPosition: pgErr.InternalPosition,
			InternalQuery:    pgErr.InternalQuery,
			Where:            pgErr.Where,
			SchemaName:       pgErr.SchemaName,
			TableName:        pgErr.TableName,
			ColumnName:       pgErr.ColumnName,
			DataTypeName:     pgErr.DataTypeName,
			ConstraintName:   pgErr.ConstraintName,
			File:             pgErr.File,
			Line:             pgErr.Line,
			Routine:          pgErr.Routine,
		}
	} else {
		// Generic error
		er = &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000", // internal_error
			Message:  err.Error(),
		}
	}

	return writeBackendMessage(w, er, timeout)
}

// writeErrorAndReady writes an error response followed by ReadyForQuery
// If timeout is 0, uses the default from env var
func writeErrorAndReady(w io.Writer, err error, txStatus byte, timeout time.Duration) error {
	if err := writeErrorResponse(w, err, timeout); err != nil {
		return err
	}

	return writeReadyForQuery(w, txStatus, timeout)
}

// writeProtoError writes a protocol error with a specific code
// If timeout is 0, uses the default from env var
func writeProtoError(w io.Writer, code, message string, timeout time.Duration) error {
	return writeBackendMessage(w, &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	}, timeout)
}
