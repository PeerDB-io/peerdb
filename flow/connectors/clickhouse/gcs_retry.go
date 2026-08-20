package connclickhouse

import (
	"strings"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
)

// isRetryableGCSError checks if the error is a retryable GCS authentication error.
// This handles intermittent 401 Unauthorized errors that can occur due to
// Google Cloud's eventual consistency with newly minted downscoped OAuth2 tokens.
//
// The error must be:
// 1. A ClickHouse exception with code ErrReceivedErrorFromRemoteIOServer (86)
// 2. From storage.googleapis.com
// 3. An HTTP 401 Unauthorized error
func isRetryableGCSError(err error) bool {
	if err == nil {
		return false
	}

	ex, ok := err.(*clickhouse.Exception)
	if !ok || ex == nil {
		return false
	}

	// Check for error code 86: RECEIVED_ERROR_FROM_REMOTE_IO_SERVER
	if chproto.Error(ex.Code) != chproto.ErrReceivedErrorFromRemoteIOServer {
		return false
	}

	msg := ex.Message
	return strings.Contains(msg, "storage.googleapis.com") &&
		strings.Contains(msg, "HTTP status code: 401")
}
