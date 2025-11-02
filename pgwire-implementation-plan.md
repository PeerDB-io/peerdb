# PgWire Proxy Implementation Plan for PeerDB

## Overview
Implement a minimal PostgreSQL wire protocol proxy that accepts psql connections and forwards queries to an upstream PostgreSQL database using simple protocol only.

## Step-by-Step Implementation Plan

### Phase 1: Core Infrastructure Setup

#### Step 1: Create Package Structure
1. Create directory `/flow/pgwire/`
2. Create initial file structure:
   - `config.go` - Configuration structures and validation
   - `server.go` - Main server implementation
   - `session.go` - Per-client session handler
   - `protocol.go` - Protocol message helpers
   - `upstream.go` - Upstream connection management
   - `errors.go` - Error translation utilities
   - `metrics.go` - Metrics and observability

#### Step 2: Add Dependencies
1. Add `github.com/jackc/pgproto3/v2` to go.mod for protocol handling
2. Run `go mod tidy` to update dependencies

#### Step 3: Create Configuration Structure
```go
// flow/pgwire/config.go
type ServerConfig struct {
    ListenAddress  string
    UpstreamDSN    string
    EnableTLS      bool
    TLSCertPath    string
    TLSKeyPath     string
    MaxConnections int
    QueryTimeout   time.Duration
    MaxRows        int
    MaxBytes       int
}
```

### Phase 2: Protocol Implementation

#### Step 4: Implement Protocol Helpers
1. Create SSL negotiation handler:
   - Handle SSLRequest message
   - Respond with 'N' for plaintext or 'S' for TLS
   - Implement TLS wrapper if enabled

2. Create startup sequence handler:
   - Parse StartupMessage
   - Send AuthenticationOk
   - Send required ParameterStatus messages
   - Send BackendKeyData from upstream
   - Send ReadyForQuery

3. Create query handler:
   - Receive Query messages
   - Forward to upstream via SimpleQuery
   - Stream results back

#### Step 5: Implement Session Handler
1. Create Session struct:
   ```go
   type Session struct {
       id         uint32
       clientConn net.Conn
       upstream   *pgx.Conn
       pgConn     *pgconn.PgConn
       pid        uint32
       secret     uint32
       config     *ServerConfig
       logger     *slog.Logger
   }
   ```

2. Implement main session loop:
   - SSL negotiation
   - Startup handshake
   - Query processing loop
   - Terminate handling
   - Error handling

#### Step 6: Implement Upstream Connection Management
1. Create connection factory using pgx.ConnectConfig
2. Set runtime parameters:
   - statement_timeout
   - idle_in_transaction_session_timeout
   - application_name for audit
3. Extract PID and SecretKey for cancel handling

### Phase 3: Server Implementation

#### Step 7: Implement Main Server
1. Create Server struct:
   ```go
   type Server struct {
       config     *ServerConfig
       listener   net.Listener
       sessions   sync.Map // pid -> *Session
       logger     *slog.Logger
       shutdownCh chan struct{}
   }
   ```

2. Implement ListenAndServe:
   - Create TCP listener
   - Accept loop with goroutine per connection
   - Connection limit enforcement
   - Graceful shutdown handling

#### Step 8: Add Cancel Request Support
1. Create cancel handler:
   - Map pid/secret to sessions
   - Open raw connection to upstream
   - Send CancelRequest packet
   - Close connection

### Phase 4: Safety and Observability

#### Step 9: Implement Guardrails
1. Create `guardrails.go` with:
   - Query validation (deny/allow lists)
   - Row count limits
   - Byte count limits
   - Query timeout enforcement

2. Add limit checks during result streaming:
   - Track rows/bytes sent
   - Stop with error if limits exceeded

#### Step 10: Add Error Translation
1. Create error handler:
   - Convert pgconn.PgError to ErrorResponse
   - Include all error fields (severity, code, message, detail, hint)
   - Send ReadyForQuery after error

#### Step 11: Implement Metrics
1. Create metrics structure:
   - Active connections counter
   - Query count/duration histograms
   - Error counters
   - Bytes/rows sent counters

2. Integrate with OtelManager if enabled

### Phase 5: CLI Integration

#### Step 12: Create Entry Point
1. Create `/flow/cmd/pgwire_proxy.go`:
   ```go
   type PgWireProxyParams struct {
       Port              uint16
       UpstreamDSN       string
       EnableTLS         bool
       TLSCert           string
       TLSKey            string
       MaxConnections    int
       QueryTimeout      time.Duration
       MaxRows           int
       MaxBytes          int
       EnableOtelMetrics bool
   }

   func PgWireProxyMain(ctx context.Context, params *PgWireProxyParams) error
   ```

#### Step 13: Add CLI Command
1. Modify `/flow/main.go` to add new command:
   - Command name: `pgwire-proxy`
   - Add all configuration flags
   - Wire up to PgWireProxyMain

### Phase 6: Testing

#### Step 14: Write Unit Tests
1. Create test files:
   - `server_test.go` - Server lifecycle tests
   - `session_test.go` - Session handling tests
   - `protocol_test.go` - Protocol message tests
   - `guardrails_test.go` - Limit enforcement tests

2. Test scenarios:
   - Protocol message encoding/decoding
   - Error translation
   - Guardrail limits
   - Connection limits
   - Graceful shutdown

#### Step 15: Create Integration Test Documentation
1. Create `/flow/e2e/pgwire_test_plan.md` with:
   - Test environment setup
   - Connection test cases
   - Query execution scenarios
   - Error handling tests
   - Cancel request tests
   - Transaction tests
   - Performance tests

### Phase 7: Documentation and Deployment

#### Step 16: Add Documentation
1. Create README in `/flow/pgwire/README.md`
2. Document configuration options
3. Add usage examples
4. Document security model

#### Step 17: Update Docker Compose
1. Add pgwire-proxy service to docker-compose-dev.yml
2. Configure with example upstream DSN
3. Expose on different port (e.g., 5433)

## Implementation Order

### Day 1: Foundation
1. Create package structure (Step 1)
2. Add dependencies (Step 2)
3. Create configuration (Step 3)
4. Implement basic protocol helpers (Step 4 - partial)

### Day 2: Core Protocol
1. Complete protocol helpers (Step 4)
2. Implement session handler (Step 5)
3. Implement upstream connection (Step 6)
4. Basic server implementation (Step 7)

### Day 3: Features and Safety
1. Add cancel support (Step 8)
2. Implement guardrails (Step 9)
3. Add error translation (Step 10)
4. Add metrics (Step 11)

### Day 4: Integration and Testing
1. Create CLI entry point (Step 12)
2. Add CLI command (Step 13)
3. Write unit tests (Step 14)
4. Create integration test plan (Step 15)

### Day 5: Polish and Documentation
1. Add documentation (Step 16)
2. Update Docker Compose (Step 17)
3. Manual testing and bug fixes
4. Performance optimization

## Key Implementation Notes

### Protocol Details
- Only support simple protocol (Query messages)
- Reject extended protocol (Parse/Bind/Execute)
- Reject COPY protocol
- Handle multi-statement queries (semicolon-separated)

### Security Model
- No per-user authentication (trust infrastructure)
- Optional server-side TLS only
- Upstream credentials from DSN configuration
- Audit via application_name in PostgreSQL logs

### Resource Limits
- Default 30s query timeout
- Default 10,000 row limit
- Default 100MB byte limit
- Default 100 max connections
- All limits configurable via CLI flags

### Error Handling
- Translate all upstream errors to proper ErrorResponse
- Always follow errors with ReadyForQuery
- Log all errors with context
- Include query duration in error logs

### Transaction Support
- Single upstream connection per client
- Transactions work naturally (BEGIN/COMMIT/ROLLBACK)
- Track transaction state for ReadyForQuery status

## Success Criteria
1. psql can connect and execute queries
2. \d commands work (catalog queries)
3. Multi-statement queries work
4. UPDATE/INSERT/DELETE with RETURNING work
5. Transactions work properly
6. Cancel requests work (Ctrl+C)
7. Errors are properly translated
8. Resource limits are enforced
9. Metrics are collected
10. Graceful shutdown works

## Testing Checklist

### Unit Tests
- [ ] Protocol message encoding/decoding
- [ ] SSL negotiation
- [ ] Startup sequence
- [ ] Query handling
- [ ] Error translation
- [ ] Guardrail enforcement
- [ ] Cancel request handling
- [ ] Connection limits
- [ ] Graceful shutdown

### Manual Testing (via psql)
- [ ] Basic SELECT query
- [ ] Multi-statement query
- [ ] Catalog commands (\d, \dt, \l)
- [ ] DML with RETURNING
- [ ] Transaction block
- [ ] Query cancellation (Ctrl+C)
- [ ] Large result truncation
- [ ] Syntax error handling
- [ ] Permission error handling
- [ ] Timeout handling

### Load Testing
- [ ] Concurrent connections up to limit
- [ ] Connection rejection at limit
- [ ] Query throughput
- [ ] Memory usage under load
- [ ] Graceful degradation

## Risk Mitigation
1. **Protocol Compatibility**: Test with multiple psql versions
2. **Resource Exhaustion**: Strict limits with fail-safe defaults
3. **SQL Injection**: No query parsing/modification, pure forwarding
4. **Connection Leaks**: Proper cleanup in defer blocks
5. **Panic Recovery**: Recover in session handlers to prevent crashes