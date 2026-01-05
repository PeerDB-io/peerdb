package pgwire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

// Server is the main PgWire proxy server
type Server struct {
	listener    net.Listener
	catalogPool shared.CatalogPool
	logger      *slog.Logger
	shutdownCh  chan struct{}
	sessions    sync.Map
	wg          sync.WaitGroup
	activeConns atomic.Int32
	port        uint16
}

// NewServer creates a new PgWire proxy server
func NewServer(catalogPool shared.CatalogPool, port uint16) *Server {
	return &Server{
		catalogPool: catalogPool,
		logger:      slog.Default(),
		port:        port,
		shutdownCh:  make(chan struct{}),
	}
}

// ListenAndServe starts the server and blocks until shutdown
func (s *Server) ListenAndServe(ctx context.Context) error {
	listenAddress := fmt.Sprintf(":%d", s.port)

	// Create TCP listener
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddress, err)
	}
	s.listener = listener

	s.logger.InfoContext(ctx, "PgWire proxy server listening",
		slog.String("address", listenAddress),
	)

	// Handle shutdown signal
	go func() {
		<-ctx.Done()
		s.logger.InfoContext(context.Background(), "Shutdown signal received, closing listener")
		close(s.shutdownCh)
		_ = s.listener.Close()
	}()

	// Accept loop
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				// Shutdown requested
				s.logger.InfoContext(ctx, "Waiting for active connections to close",
					slog.Int("active", int(s.activeConns.Load())),
				)
				s.wg.Wait()
				s.logger.InfoContext(ctx, "All connections closed, shutdown complete")
				return nil
			default:
				s.logger.ErrorContext(ctx, "Failed to accept connection", slog.Any("error", err))
				continue
			}
		}

		// Handle connection in goroutine
		s.wg.Add(1)
		s.activeConns.Add(1)

		go func(conn net.Conn) {
			defer func() {
				s.activeConns.Add(-1)
				s.wg.Done()
			}()

			s.handleConnection(ctx, conn)
		}(conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	// Add panic recovery to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			s.logger.ErrorContext(ctx, "Panic in session handler",
				slog.Any("panic", r),
				slog.String("remote_addr", conn.RemoteAddr().String()),
			)
		}
	}()

	// Set connection deadline to prevent hanging on startup
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	session := NewSession(conn, s.catalogPool, s.logger)

	// Handle the session - this will set pid/secret and register for cancels
	if err := session.Handle(ctx, s); err != nil {
		// Check if it's a cancel request during startup
		var cancelErr *CancelRequestError
		if errors.As(err, &cancelErr) {
			s.handleCancelRequest(cancelErr.ProcessID, cancelErr.SecretKey)
			return
		}
		session.logger.ErrorContext(ctx, "Session error", slog.Any("error", err))
	}
}

// handleCancelRequest processes a cancel request
func (s *Server) handleCancelRequest(pid, secret uint32) {
	ctx := context.Background()

	key := [2]uint32{pid, secret}
	if val, ok := s.sessions.Load(key); ok {
		session := val.(*Session)
		s.logger.InfoContext(ctx, "Processing cancel request",
			slog.Uint64("pid", uint64(pid)),
			slog.Uint64("secret", uint64(secret)),
		)

		// Send cancel to upstream using the interface's Cancel method
		if session.upstream != nil {
			if err := session.upstream.Cancel(ctx); err != nil {
				s.logger.WarnContext(ctx, "Cancel request failed",
					slog.Uint64("pid", uint64(pid)),
					slog.Any("error", err),
				)
			}
		}
	} else {
		s.logger.WarnContext(ctx, "Cancel request for unknown session",
			slog.Uint64("pid", uint64(pid)),
			slog.Uint64("secret", uint64(secret)),
		)
	}
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.InfoContext(ctx, "Shutting down server")

	// Close listener to stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	// Wait for active connections to finish with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.InfoContext(ctx, "All connections closed gracefully")
		return nil
	case <-ctx.Done():
		s.logger.WarnContext(ctx, "Shutdown timeout, forcing close",
			slog.Int("active", int(s.activeConns.Load())),
		)
		return ctx.Err()
	}
}
