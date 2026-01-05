package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

var sessionIDCounter atomic.Uint32

// Session represents a client session
type Session struct {
	clientConn   net.Conn
	catalogPool  shared.CatalogPool
	backend      *pgproto3.Backend
	upstream     Upstream
	logger       *slog.Logger
	guardrails   *Guardrails
	peerName     string
	queryTimeout time.Duration
	idleTimeout  time.Duration
	writeTimeout time.Duration
	pid          uint32
	secret       uint32
	id           uint32
}

// startupConfig holds parsed configuration from startup parameters
type startupConfig struct {
	maxRows      int64
	maxBytes     int64
	queryTimeout time.Duration
	idleTimeout  time.Duration
	writeTimeout time.Duration
}

// parseStartupOptions parses PostgreSQL options string (-c key=value format)
// and extracts peerdb.* parameters
func parseStartupOptions(options string) map[string]string {
	result := make(map[string]string)
	if options == "" {
		return result
	}

	// Split on -c, handling the format: -c key=value -c key2=value2
	parts := strings.Split(options, "-c")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split on first = sign
		if idx := strings.Index(part, "="); idx > 0 {
			key := strings.TrimSpace(part[:idx])
			value := strings.TrimSpace(part[idx+1:])
			result[key] = value
		}
	}

	return result
}

// parseStartupConfig extracts guardrail config from startup parameters
func parseStartupConfig(params map[string]string) *startupConfig {
	cfg := &startupConfig{
		maxRows:      internal.PeerDBPgwireMaxRows(),
		maxBytes:     internal.PeerDBPgwireMaxBytes(),
		queryTimeout: time.Duration(internal.PeerDBPgwireQueryTimeoutSeconds()) * time.Second,
		writeTimeout: time.Duration(internal.PeerDBPgwireWriteTimeoutSeconds()) * time.Second,
		idleTimeout:  30 * time.Minute,
	}

	// Parse options string for -c parameters
	options := parseStartupOptions(params["options"])

	// Check for peerdb.max_rows
	if v, ok := options["peerdb.max_rows"]; ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.maxRows = parsed
		}
	}

	// Check for peerdb.max_bytes
	if v, ok := options["peerdb.max_bytes"]; ok {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.maxBytes = parsed
		}
	}

	// Check for peerdb.query_timeout (in seconds)
	if v, ok := options["peerdb.query_timeout"]; ok {
		if parsed, err := strconv.Atoi(v); err == nil {
			cfg.queryTimeout = time.Duration(parsed) * time.Second
		}
	}

	// Check for peerdb.write_timeout (in seconds)
	if v, ok := options["peerdb.write_timeout"]; ok {
		if parsed, err := strconv.Atoi(v); err == nil {
			cfg.writeTimeout = time.Duration(parsed) * time.Second
		}
	}

	// Check for peerdb.idle_timeout (in seconds)
	if v, ok := options["peerdb.idle_timeout"]; ok {
		if parsed, err := strconv.Atoi(v); err == nil {
			cfg.idleTimeout = time.Duration(parsed) * time.Second
		}
	}

	return cfg
}

// NewSession creates a new session
func NewSession(clientConn net.Conn, catalogPool shared.CatalogPool, logger *slog.Logger) *Session {
	id := sessionIDCounter.Add(1)

	return &Session{
		id:          id,
		clientConn:  clientConn,
		catalogPool: catalogPool,
		logger: logger.With(
			slog.Uint64("session_id", uint64(id)),
			slog.String("client_addr", clientConn.RemoteAddr().String()),
		),
		// guardrails, queryTimeout, idleTimeout are set in Handle() after parsing startup params
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
			_ = s.upstream.Close()
		}
		if s.clientConn != nil {
			_ = s.clientConn.Close()
		}

		s.logger.InfoContext(ctx, "Session ended")
	}()

	s.logger.InfoContext(ctx, "New session started")

	// Handle SSL negotiation and startup
	newConn, backend, startup, err := acceptStartup(s.clientConn)
	if err != nil {
		// Check if it's a cancel request - return as-is to be handled by server
		var cancelErr *CancelRequestError
		if errors.As(err, &cancelErr) {
			s.logger.InfoContext(ctx, "Cancel request during startup",
				slog.Uint64("pid", uint64(cancelErr.ProcessID)),
				slog.Uint64("secret", uint64(cancelErr.SecretKey)))
			return err
		}
		s.logger.ErrorContext(ctx, "Startup failed", slog.Any("error", err))
		return err
	}
	s.clientConn = newConn
	s.backend = backend

	// Extract peer name from database parameter
	s.peerName = startup.Parameters["database"]
	s.logger = s.logger.With(slog.String("peer", s.peerName))

	// Parse startup parameters for guardrails config
	cfg := parseStartupConfig(startup.Parameters)
	s.queryTimeout = cfg.queryTimeout
	s.idleTimeout = cfg.idleTimeout
	s.writeTimeout = cfg.writeTimeout
	s.guardrails = NewGuardrails(cfg.maxRows, cfg.maxBytes)

	s.logger.InfoContext(ctx, "Received startup message",
		slog.String("user", startup.Parameters["user"]),
		slog.String("database", startup.Parameters["database"]),
		slog.String("application_name", startup.Parameters["application_name"]),
		slog.Int64("max_rows", cfg.maxRows),
		slog.Int64("max_bytes", cfg.maxBytes),
		slog.Duration("query_timeout", cfg.queryTimeout),
		slog.Duration("idle_timeout", cfg.idleTimeout),
		slog.Duration("write_timeout", cfg.writeTimeout),
	)

	// Authenticate client using SCRAM-SHA-256
	if err := authenticateSCRAM(s.clientConn, s.writeTimeout); err != nil {
		s.logger.WarnContext(ctx, "Authentication failed", slog.Any("error", err))
		_ = writeProtoError(s.clientConn, "28P01", "password authentication failed", s.writeTimeout)
		return err
	}

	// Create upstream connection using factory function
	s.upstream, err = NewUpstream(ctx, s.catalogPool, s.peerName, s.queryTimeout)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to connect to upstream", slog.Any("error", err))
		if werr := writeProtoError(s.clientConn, "3D000", err.Error(), s.writeTimeout); werr != nil {
			s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
		}
		return err
	}

	// Get backend key data for cancel support
	s.pid, s.secret = s.upstream.BackendKeyData()

	// Register session for cancel handling with compound key
	key := [2]uint32{s.pid, s.secret}
	server.sessions.Store(key, s)

	s.logger.InfoContext(ctx, "Connected to upstream",
		slog.Uint64("upstream_pid", uint64(s.pid)),
	)

	// Get server parameters from upstream
	serverParams := s.upstream.ServerParameters(ctx)

	// Send greeting to client with actual server parameters
	if err := sendGreeting(s.clientConn, s.pid, s.secret, serverParams, s.txStatus(), s.writeTimeout); err != nil {
		s.logger.ErrorContext(ctx, "Failed to send greeting", slog.Any("error", err))
		return err
	}

	// Clear connection deadlines after successful handshake
	_ = s.clientConn.SetDeadline(time.Time{})

	// Main message loop
	return s.messageLoop(ctx)
}

// messageLoop processes client messages
func (s *Session) messageLoop(ctx context.Context) error {
	for {
		// Set read deadline for idle timeout
		_ = s.clientConn.SetReadDeadline(time.Now().Add(s.idleTimeout))

		msg, err := s.backend.Receive()
		if err != nil {
			if err == io.EOF {
				s.logger.InfoContext(ctx, "Client disconnected")
				return nil
			}
			// Check if it's a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.InfoContext(ctx, "Client idle timeout")
				if werr := writeProtoError(s.clientConn, "57P05", "idle timeout exceeded", s.writeTimeout); werr != nil {
					s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
				}
				return err
			}
			s.logger.ErrorContext(ctx, "Error receiving message", slog.Any("error", err))
			return err
		}

		// Type switch on FrontendMessage (messages FROM client)
		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := s.handleQuery(ctx, m.String); err != nil {
				s.logger.ErrorContext(ctx, "Query failed", slog.Any("error", err))
			}

		case *pgproto3.Terminate:
			s.logger.InfoContext(ctx, "Client requested termination")
			return nil

		default:
			// Reject extended protocol and other unsupported messages
			s.logger.WarnContext(ctx, "Unsupported message type", slog.String("type", fmt.Sprintf("%T", m)))
			errMsg := fmt.Sprintf("extended protocol not supported: %T", m)
			if werr := writeProtoError(s.clientConn, "0A000", errMsg, s.writeTimeout); werr != nil {
				s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
			}
			if werr := writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout); werr != nil {
				s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
			}
		}
	}
}

// handleQuery processes a query message
func (s *Session) handleQuery(ctx context.Context, query string) error {
	startTime := time.Now()

	// Handle empty queries as no-op (return EmptyQueryResponse)
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || strings.Trim(trimmed, ";") == "" {
		if err := writeBackendMessage(s.clientConn, &pgproto3.EmptyQueryResponse{}, s.writeTimeout); err != nil {
			return err
		}
		return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
	}

	// Reset guardrails for new query
	s.guardrails.Reset()

	// Check query against upstream-specific security rules
	if err := s.upstream.CheckQuery(query); err != nil {
		s.logger.WarnContext(ctx, "Query blocked by guardrails",
			slog.String("query", truncateQuery(query, 100)),
			slog.Any("error", err))
		if werr := writeProtoError(s.clientConn, "54000", err.Error(), s.writeTimeout); werr != nil {
			s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
		}
		return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
	}

	s.logger.InfoContext(ctx, "Executing query",
		slog.String("query", truncateQuery(query, 200)),
	)

	// Create query context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	// Execute query using the upstream interface
	resultIter, err := s.upstream.Exec(queryCtx, query)
	if err != nil {
		if werr := writeErrorResponse(s.clientConn, err, s.writeTimeout); werr != nil {
			s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
		}
		return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
	}

	// Process each result set
	for resultIter.NextResult() {
		fieldDescs := resultIter.FieldDescriptions()

		// Send RowDescription if this result set returns rows
		if len(fieldDescs) > 0 {
			// Convert to pgproto3.FieldDescription
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
			if err := writeBackendMessage(s.clientConn, rowDesc, s.writeTimeout); err != nil {
				return err
			}

			// Stream DataRows one at a time - no buffering
			for resultIter.NextRow() {
				// Check row limit BEFORE processing
				if err := s.guardrails.AddRow(); err != nil {
					s.logger.WarnContext(ctx, "Row limit exceeded", slog.Any("error", err))
					cancel()
					_ = resultIter.CloseAll()
					if werr := writeProtoError(s.clientConn, "54000", err.Error(), s.writeTimeout); werr != nil {
						s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
					}
					return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
				}

				row := resultIter.RowValues()

				// Check byte limit - include wire protocol overhead
				const perFieldOverhead = 4 // int32 length prefix per field
				const dataRowOverhead = 6  // message type (1) + message length (4) + field count (2)

				rowBytes := int64(dataRowOverhead)
				for _, val := range row {
					rowBytes += perFieldOverhead
					if val != nil {
						rowBytes += int64(len(val))
					}
				}

				if err := s.guardrails.AddBytes(rowBytes); err != nil {
					s.logger.WarnContext(ctx, "Byte limit exceeded", slog.Any("error", err))
					cancel()
					_ = resultIter.CloseAll()
					if werr := writeProtoError(s.clientConn, "54000", err.Error(), s.writeTimeout); werr != nil {
						s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
					}
					return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
				}

				// Send DataRow
				dataRow := &pgproto3.DataRow{Values: row}
				if err := writeBackendMessage(s.clientConn, dataRow, s.writeTimeout); err != nil {
					return err
				}
			}
		}

		// Close the result reader to get the CommandTag
		resultIter.Close()
		if err := resultIter.Err(); err != nil {
			if werr := writeErrorResponse(s.clientConn, err, s.writeTimeout); werr != nil {
				s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
			}
			return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
		}

		// Send EmptyQueryResponse for empty statements, CommandComplete otherwise
		commandTagStr := resultIter.CommandTag()
		if commandTagStr == "" {
			// Empty query - send EmptyQueryResponse per protocol
			emptyResp := &pgproto3.EmptyQueryResponse{}
			if err := writeBackendMessage(s.clientConn, emptyResp, s.writeTimeout); err != nil {
				return err
			}
		} else {
			// Normal statement - send CommandComplete
			cmdComplete := &pgproto3.CommandComplete{
				CommandTag: []byte(commandTagStr),
			}
			if err := writeBackendMessage(s.clientConn, cmdComplete, s.writeTimeout); err != nil {
				return err
			}
		}
	}

	// Close entire multi-result and check for final errors
	if err := resultIter.CloseAll(); err != nil {
		if werr := writeErrorResponse(s.clientConn, err, s.writeTimeout); werr != nil {
			s.logger.WarnContext(ctx, "Failed to write to client", slog.Any("error", werr))
		}
		return writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout)
	}

	// Send ReadyForQuery once after all result sets are processed
	if err := writeReadyForQuery(s.clientConn, s.txStatus(), s.writeTimeout); err != nil {
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
	return s.upstream.TxStatus()
}

// truncateQuery truncates a query string for logging
func truncateQuery(query string, maxLen int) string {
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
