package pgwire

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgproto3/v2"
)

// CancelRequest represents a cancel request from a client
type CancelRequest struct {
	ProcessID uint32
	SecretKey uint32
}

// CancelRequestError is a typed error returned when a cancel request is received during startup
type CancelRequestError struct {
	PID    uint32
	Secret uint32
}

func (e *CancelRequestError) Error() string {
	return "cancel request received"
}

// acceptStartup handles SSL negotiation and startup message in one clean loop
// This is the correct way to handle the startup phase on the server side
// Returns a CancelRequest if the client sent a cancel request instead of a startup
func acceptStartup(conn net.Conn, tlsConfig *tls.Config) (net.Conn, *pgproto3.Backend, *pgproto3.StartupMessage, *CancelRequest, error) {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to receive startup message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if tlsConfig != nil {
				// Send 'S' to indicate we support SSL
				if _, err := conn.Write([]byte{'S'}); err != nil {
					return nil, nil, nil, nil, fmt.Errorf("failed to send SSL acknowledgment: %w", err)
				}

				// Upgrade connection to TLS
				tlsConn := tls.Server(conn, tlsConfig)
				if err := tlsConn.Handshake(); err != nil {
					return nil, nil, nil, nil, fmt.Errorf("TLS handshake failed: %w", err)
				}

				// Create new backend on TLS connection
				backend = pgproto3.NewBackend(pgproto3.NewChunkReader(tlsConn), tlsConn)
				conn = tlsConn
				// Loop to receive actual StartupMessage
			} else {
				// Send 'N' to indicate we don't support SSL
				if _, err := conn.Write([]byte{'N'}); err != nil {
					return nil, nil, nil, nil, fmt.Errorf("failed to send SSL rejection: %w", err)
				}
				// Loop to receive actual StartupMessage
			}

		case *pgproto3.GSSEncRequest:
			// We don't support GSS encryption - respond with 'N'
			// This is safe and allows clients to fall back to other methods
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("failed to send GSSENC rejection: %w", err)
			}
			// Loop to receive actual StartupMessage

		case *pgproto3.CancelRequest:
			// Return typed cancel request to let the server handle it
			cancel := &CancelRequest{
				ProcessID: uint32(m.ProcessID),
				SecretKey: uint32(m.SecretKey),
			}
			return conn, backend, nil, cancel, nil

		case *pgproto3.StartupMessage:
			// This is what we're waiting for
			return conn, backend, m, nil, nil

		default:
			return nil, nil, nil, nil, fmt.Errorf("unexpected startup message type: %T", m)
		}
	}
}

// sendGreeting sends the complete startup greeting sequence to the client
func sendGreeting(conn io.Writer, pid, secret uint32, params map[string]string, txStatus byte) error {
	write := func(m pgproto3.BackendMessage) error {
		buf, err := m.Encode(nil)
		if err != nil {
			return err
		}

		// Set write deadline if this is a net.Conn
		if netConn, ok := conn.(net.Conn); ok {
			_ = netConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			defer func() {
				_ = netConn.SetWriteDeadline(time.Time{})
			}()
		}

		_, err = conn.Write(buf)
		return err
	}

	// 1. AuthenticationOk
	if err := write(&pgproto3.AuthenticationOk{}); err != nil {
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
		if err := write(&pgproto3.ParameterStatus{Name: k, Value: v}); err != nil {
			return fmt.Errorf("failed to send ParameterStatus: %w", err)
		}
	}

	// 3. BackendKeyData (for cancel support)
	if err := write(&pgproto3.BackendKeyData{
		ProcessID: pid,
		SecretKey: secret,
	}); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// 4. ReadyForQuery
	if err := write(&pgproto3.ReadyForQuery{TxStatus: txStatus}); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// writeBackendMessage is a helper to encode and write a backend message
// Sets write deadline to prevent blocking on slow/dead clients
func writeBackendMessage(conn io.Writer, msg pgproto3.BackendMessage) error {
	buf, err := msg.Encode(nil)
	if err != nil {
		return err
	}

	// Set write deadline if this is a net.Conn
	if netConn, ok := conn.(net.Conn); ok {
		_ = netConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		defer func() {
			// Clear deadline after write
			_ = netConn.SetWriteDeadline(time.Time{})
		}()
	}

	_, err = conn.Write(buf)
	return err
}

// writeReadyForQuery sends a ReadyForQuery message
func writeReadyForQuery(conn io.Writer, txStatus byte) error {
	return writeBackendMessage(conn, &pgproto3.ReadyForQuery{TxStatus: txStatus})
}
