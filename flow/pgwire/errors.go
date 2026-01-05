package pgwire

import (
	"errors"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// writeErrorResponse converts an error to a PostgreSQL ErrorResponse and writes it to the connection
// Expects errors to be wrapped as UpstreamError by upstream implementations
// If timeout is 0, uses the default from env var
func writeErrorResponse(w io.Writer, err error, timeout time.Duration) error {
	var upstreamErr *UpstreamError
	var resp *pgproto3.ErrorResponse

	if errors.As(err, &upstreamErr) {
		resp = upstreamErr.Resp
	} else {
		// Fallback for non-upstream errors
		resp = &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "XX000",
			Message:  err.Error(),
		}
	}

	return writeBackendMessage(w, resp, timeout)
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
