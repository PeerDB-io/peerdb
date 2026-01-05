package pgwire

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

// defaultWriteTimeout returns the default write deadline from env var
func defaultWriteTimeout() time.Duration {
	return time.Duration(internal.PeerDBPgwireWriteTimeoutSeconds()) * time.Second
}

// CancelRequestError is returned when a cancel request is received during startup
type CancelRequestError struct {
	ProcessID uint32
	SecretKey uint32
}

func (e *CancelRequestError) Error() string {
	return "cancel request received"
}

// acceptStartup handles SSL negotiation and startup message
// TLS is not supported - network is assumed to be secure
// Returns CancelRequestError if the client sent a cancel request instead of a startup
func acceptStartup(conn net.Conn) (net.Conn, *pgproto3.Backend, *pgproto3.StartupMessage, error) {
	backend := pgproto3.NewBackend(conn, conn)

	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to receive startup message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			// TLS not supported - network is tailscale-guarded
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, nil, nil, fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			// Loop to receive actual StartupMessage

		case *pgproto3.GSSEncRequest:
			// We don't support GSS encryption - respond with 'N'
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, nil, nil, fmt.Errorf("failed to send GSSENC rejection: %w", err)
			}
			// Loop to receive actual StartupMessage

		case *pgproto3.CancelRequest:
			// Return as error to let the server handle it
			return conn, backend, nil, &CancelRequestError{
				ProcessID: m.ProcessID,
				SecretKey: m.SecretKey,
			}

		case *pgproto3.StartupMessage:
			// This is what we're waiting for
			return conn, backend, m, nil

		default:
			return nil, nil, nil, fmt.Errorf("unexpected startup message type: %T", m)
		}
	}
}

// sendGreeting sends the complete startup greeting sequence to the client
// If writeTimeout is 0, uses the default from env var
func sendGreeting(conn io.Writer, pid, secret uint32, params map[string]string, txStatus byte, writeTimeout time.Duration) error {
	// 1. AuthenticationOk
	if err := writeBackendMessage(conn, &pgproto3.AuthenticationOk{}, writeTimeout); err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}

	// 2. ParameterStatus messages - use real values from upstream
	// Only provide defaults for values not supplied
	defaults := map[string]string{
		"client_encoding":             "UTF8",
		"server_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"standard_conforming_strings": "on",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
	}

	// Start with defaults, then override with actual upstream params
	finalParams := make(map[string]string)
	for k, v := range defaults {
		finalParams[k] = v
	}
	for k, v := range params {
		finalParams[k] = v
	}

	for k, v := range finalParams {
		if err := writeBackendMessage(conn, &pgproto3.ParameterStatus{Name: k, Value: v}, writeTimeout); err != nil {
			return fmt.Errorf("failed to send ParameterStatus: %w", err)
		}
	}

	// 3. BackendKeyData (for cancel support)
	if err := writeBackendMessage(conn, &pgproto3.BackendKeyData{
		ProcessID: pid,
		SecretKey: secret,
	}, writeTimeout); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// 4. ReadyForQuery
	if err := writeBackendMessage(conn, &pgproto3.ReadyForQuery{TxStatus: txStatus}, writeTimeout); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// writeBackendMessage is a helper to encode and write a backend message
// Sets write deadline to prevent blocking on slow/dead clients
// If timeout is 0, uses the default from env var
func writeBackendMessage(conn io.Writer, msg pgproto3.BackendMessage, timeout time.Duration) error {
	buf, err := msg.Encode(nil)
	if err != nil {
		return err
	}

	if timeout == 0 {
		timeout = defaultWriteTimeout()
	}

	// Set write deadline if this is a net.Conn
	if netConn, ok := conn.(net.Conn); ok {
		_ = netConn.SetWriteDeadline(time.Now().Add(timeout))
		defer func() {
			_ = netConn.SetWriteDeadline(time.Time{}) // Clear deadline after write
		}()
	}

	_, err = conn.Write(buf)
	return err
}

// writeReadyForQuery sends a ReadyForQuery message
func writeReadyForQuery(conn io.Writer, txStatus byte, timeout time.Duration) error {
	return writeBackendMessage(conn, &pgproto3.ReadyForQuery{TxStatus: txStatus}, timeout)
}
