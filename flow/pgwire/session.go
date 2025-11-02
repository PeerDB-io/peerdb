package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5"
)

var sessionIDCounter atomic.Uint32

// Session represents a client session
type Session struct {
	id         uint32
	clientConn net.Conn
	backend    *pgproto3.Backend
	upstream   *pgx.Conn
	pid        uint32
	secret     uint32
	config     *ServerConfig
	logger     *slog.Logger
	metrics    *SessionMetrics
	guardrails *Guardrails
}

// NewSession creates a new session
func NewSession(clientConn net.Conn, config *ServerConfig, logger *slog.Logger) *Session {
	id := sessionIDCounter.Add(1)

	return &Session{
		id:         id,
		clientConn: clientConn,
		config:     config,
		logger: logger.With(
			slog.Uint64("session_id", uint64(id)),
			slog.String("client_addr", clientConn.RemoteAddr().String()),
		),
		metrics: &SessionMetrics{
			ConnectionTime: time.Now(),
		},
		guardrails: NewGuardrails(
			config.MaxRows,
			config.MaxBytes,
			config.DenyStatements,
			config.AllowStatements,
		),
	}
}

// Handle processes the client session
func (s *Session) Handle(ctx context.Context, server *Server) error {
	defer func() {
		// Unregister from cancel index
		if s.pid != 0 && s.secret != 0 {
			key := [2]uint32{s.pid, s.secret}
			server.sessions.Delete(key)
		}

		if s.upstream != nil {
			_ = s.upstream.Close(context.Background())
		}
		// Check for nil before closing
		if s.clientConn != nil {
			_ = s.clientConn.Close()
		}

		s.logger.InfoContext(ctx, "Session ended",
			slog.Int64("queries", s.metrics.QueriesTotal),
			slog.Int64("rows_sent", s.metrics.RowsSent),
			slog.Int64("bytes_sent", s.metrics.BytesSent),
			slog.Int64("errors", s.metrics.ErrorsTotal),
			slog.Duration("duration", time.Since(s.metrics.ConnectionTime)),
		)
	}()

	s.logger.InfoContext(ctx, "New session started")

	// Handle SSL negotiation and startup using the correct acceptStartup function
	var startup *pgproto3.StartupMessage
	var cancel *CancelRequest
	var err error
	// acceptStartup may return a new connection (for TLS upgrade) or nil on error
	newConn, backend, startup, cancel, err := acceptStartup(s.clientConn, s.config.TLSConfig)
	if err != nil {
		s.logger.ErrorContext(ctx, "Startup failed", slog.Any("error", err))
		return err
	}
	// Only update connection if successful
	s.clientConn = newConn
	s.backend = backend

	// Check if it's a cancel request - return to be handled by server
	if cancel != nil {
		s.logger.InfoContext(ctx, "Cancel request during startup",
			slog.Uint64("pid", uint64(cancel.ProcessID)),
			slog.Uint64("secret", uint64(cancel.SecretKey)))
		// Return a typed error that the server can detect
		return &CancelRequestError{
			PID:    cancel.ProcessID,
			Secret: cancel.SecretKey,
		}
	}

	s.logger.InfoContext(ctx, "Received startup message",
		slog.String("user", startup.Parameters["user"]),
		slog.String("database", startup.Parameters["database"]),
		slog.String("application_name", startup.Parameters["application_name"]),
	)

	// Create upstream connection
	queryTimeoutStr := fmt.Sprintf("%dms", s.config.QueryTimeout.Milliseconds())
	s.upstream, err = createUpstreamConnection(ctx, s.config.UpstreamDSN, queryTimeoutStr)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to connect to upstream", slog.Any("error", err))
		_ = writeErrorAndReady(s.clientConn, errors.New("failed to connect to upstream database"), 'I')
		return err
	}

	// Get backend key data for cancel support
	s.pid, s.secret = getBackendKeyData(s.upstream)

	// Register session for cancel handling with compound key
	key := [2]uint32{s.pid, s.secret}
	server.sessions.Store(key, s)

	s.logger.InfoContext(ctx, "Connected to upstream",
		slog.Uint64("upstream_pid", uint64(s.pid)),
	)

	// Query upstream for actual server parameters
	serverParams := queryServerParameters(ctx, s.upstream)

	// Send greeting to client with actual server parameters
	if err := sendGreeting(s.clientConn, s.pid, s.secret, serverParams, s.txStatus()); err != nil {
		s.logger.ErrorContext(ctx, "Failed to send greeting", slog.Any("error", err))
		return err
	}

	// Clear connection deadlines after successful handshake
	// The initial deadline was set in server.go to prevent hanging during startup
	// Now we clear both read and write deadlines for the main message loop
	_ = s.clientConn.SetDeadline(time.Time{})

	// Main message loop
	return s.messageLoop(ctx)
}

// messageLoop processes client messages
func (s *Session) messageLoop(ctx context.Context) error {
	// Use configured idle timeout (defaulted to 30 minutes in config validation)
	idleTimeout := s.config.IdleTimeout

	for {
		// Set read deadline for idle timeout
		_ = s.clientConn.SetReadDeadline(time.Now().Add(idleTimeout))

		msg, err := s.backend.Receive()
		if err != nil {
			if err == io.EOF {
				s.logger.InfoContext(ctx, "Client disconnected")
				return nil
			}
			// Check if it's a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.InfoContext(ctx, "Client idle timeout")
				_ = writeProtoError(s.clientConn, "57P05", "idle timeout exceeded")
				return err
			}
			s.logger.ErrorContext(ctx, "Error receiving message", slog.Any("error", err))
			return err
		}

		// Type switch on FrontendMessage (messages FROM client)
		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := s.handleQuery(ctx, m.String); err != nil {
				s.metrics.ErrorsTotal++
				s.logger.ErrorContext(ctx, "Query failed", slog.Any("error", err))
				// Error already sent in handleQuery, just continue
			}

		case *pgproto3.Terminate:
			s.logger.InfoContext(ctx, "Client requested termination")
			return nil

		default:
			// Reject extended protocol and other unsupported messages
			s.logger.WarnContext(ctx, "Unsupported message type", slog.String("type", fmt.Sprintf("%T", m)))
			_ = writeProtoError(s.clientConn, "0A000", fmt.Sprintf("extended protocol not supported: %T", m))
			_ = writeReadyForQuery(s.clientConn, s.txStatus())
		}
	}
}

// handleQuery processes a query message
func (s *Session) handleQuery(ctx context.Context, query string) error {
	startTime := time.Now()
	s.metrics.QueriesTotal++

	// Reset guardrails for new query
	s.guardrails.Reset()

	// Check query against guardrails
	if err := s.guardrails.CheckQuery(query); err != nil {
		s.logger.WarnContext(ctx, "Query blocked by guardrails",
			slog.String("query", truncateQuery(query, 100)),
			slog.Any("error", err))
		_ = writeProtoError(s.clientConn, "54000", err.Error())
		return writeReadyForQuery(s.clientConn, s.txStatus())
	}

	s.logger.InfoContext(ctx, "Executing query",
		slog.String("query", truncateQuery(query, 200)),
	)

	// Create query context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	// Execute query using pgconn Exec to properly handle multi-statement queries
	// CRITICAL: Stream results incrementally to avoid OOM on large result sets
	multiResult := s.upstream.PgConn().Exec(queryCtx, query)

	// Process each result set incrementally without buffering all rows
	for multiResult.NextResult() {
		resultReader := multiResult.ResultReader()
		fieldDescs := resultReader.FieldDescriptions()

		// Send RowDescription if this result set returns rows
		if len(fieldDescs) > 0 {
			// Convert pgconn.FieldDescription to pgproto3.FieldDescription
			protoFields := make([]pgproto3.FieldDescription, len(fieldDescs))
			for i, fd := range fieldDescs {
				protoFields[i] = pgproto3.FieldDescription{
					Name:                 []byte(fd.Name),
					TableOID:             fd.TableOID,
					TableAttributeNumber: fd.TableAttributeNumber,
					DataTypeOID:          fd.DataTypeOID,
					DataTypeSize:         fd.DataTypeSize,
					TypeModifier:         fd.TypeModifier,
					Format:               fd.Format,
				}
			}

			rowDesc := &pgproto3.RowDescription{
				Fields: protoFields,
			}
			if err := writeBackendMessage(s.clientConn, rowDesc); err != nil {
				return err
			}

			// Stream DataRows one at a time - no buffering
			for resultReader.NextRow() {
				// Check row limit BEFORE processing
				if err := s.guardrails.AddRow(); err != nil {
					s.logger.WarnContext(ctx, "Row limit exceeded", slog.Any("error", err))
					_ = writeProtoError(s.clientConn, "54000", err.Error())
					return writeReadyForQuery(s.clientConn, s.txStatus())
				}

				row := resultReader.Values()

				// Check byte limit - include wire protocol overhead
				// DataRow wire format: 1 byte message type + 4 bytes length + 2 bytes field count + per-field overhead
				const perFieldOverhead = 4 // int32 length prefix per field
				const dataRowOverhead = 6  // message type (1) + message length (4) + field count (2, though actually int16)

				rowBytes := int64(dataRowOverhead)
				for _, val := range row {
					rowBytes += perFieldOverhead
					if val != nil {
						rowBytes += int64(len(val))
					}
					// NULL values are encoded as -1 in the length field, but we count the overhead
				}

				if err := s.guardrails.AddBytes(rowBytes); err != nil {
					s.logger.WarnContext(ctx, "Byte limit exceeded", slog.Any("error", err))
					_ = writeProtoError(s.clientConn, "54000", err.Error())
					return writeReadyForQuery(s.clientConn, s.txStatus())
				}

				// Send DataRow - row is [][]byte in the right format
				dataRow := &pgproto3.DataRow{Values: row}
				if err := writeBackendMessage(s.clientConn, dataRow); err != nil {
					return err
				}

				s.metrics.RowsSent++
				s.metrics.BytesSent += rowBytes
			}
		}

		// Close the result reader to get the CommandTag
		commandTag, err := resultReader.Close()
		if err != nil {
			_ = writeErrorResponse(s.clientConn, err)
			return writeReadyForQuery(s.clientConn, s.txStatus())
		}

		// Send EmptyQueryResponse for empty statements, CommandComplete otherwise
		commandTagStr := commandTag.String()
		if commandTagStr == "" {
			// Empty query - send EmptyQueryResponse per protocol
			emptyResp := &pgproto3.EmptyQueryResponse{}
			if err := writeBackendMessage(s.clientConn, emptyResp); err != nil {
				return err
			}
		} else {
			// Normal statement - send CommandComplete
			cmdComplete := &pgproto3.CommandComplete{
				CommandTag: []byte(commandTagStr),
			}
			if err := writeBackendMessage(s.clientConn, cmdComplete); err != nil {
				return err
			}
		}
	}

	// Check for errors after all results are processed
	if err := multiResult.Close(); err != nil {
		_ = writeErrorResponse(s.clientConn, err)
		return writeReadyForQuery(s.clientConn, s.txStatus())
	}

	// Send ReadyForQuery once after all result sets are processed
	if err := writeReadyForQuery(s.clientConn, s.txStatus()); err != nil {
		return err
	}

	duration := time.Since(startTime)
	rowsSent, _ := s.guardrails.GetStats()
	s.logger.InfoContext(ctx, "Query completed",
		slog.Int64("rows", rowsSent),
		slog.Duration("duration", duration),
	)

	return nil
}

// txStatus returns the current transaction status from upstream
func (s *Session) txStatus() byte {
	if s.upstream == nil {
		return 'I' // idle
	}
	return s.upstream.PgConn().TxStatus()
}

// truncateQuery truncates a query string for logging
func truncateQuery(query string, maxLen int) string {
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
