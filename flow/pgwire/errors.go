package pgwire

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

// writeErrorResponse converts an error to a PostgreSQL ErrorResponse and writes it to the connection
// Sets write deadline to prevent blocking on slow/dead clients
func writeErrorResponse(w io.Writer, err error) error {
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

	buf, encErr := er.Encode(nil)
	if encErr != nil {
		return encErr
	}

	// Set write deadline if this is a net.Conn
	if netConn, ok := w.(net.Conn); ok {
		_ = netConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		defer func() {
			_ = netConn.SetWriteDeadline(time.Time{})
		}()
	}

	_, writeErr := w.Write(buf)
	return writeErr
}

// writeErrorAndReady writes an error response followed by ReadyForQuery
func writeErrorAndReady(w io.Writer, err error, txStatus byte) error {
	if err := writeErrorResponse(w, err); err != nil {
		return err
	}

	return writeReadyForQuery(w, txStatus)
}

// writeProtoError writes a protocol error with a specific code
// Sets write deadline to prevent blocking on slow/dead clients
func writeProtoError(w io.Writer, code, message string) error {
	er := &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	}

	buf, err := er.Encode(nil)
	if err != nil {
		return err
	}

	// Set write deadline if this is a net.Conn
	if netConn, ok := w.(net.Conn); ok {
		_ = netConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		defer func() {
			_ = netConn.SetWriteDeadline(time.Time{})
		}()
	}

	_, err = w.Write(buf)
	return err
}
