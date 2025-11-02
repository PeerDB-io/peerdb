Project Summary: Minimal Postgres pgwire Proxy (Simple-Protocol, No DB Changes)
Goal: Build a tiny Go proxy that accepts psql connections and forwards SQL to a production PostgreSQL using an existing pgx connection (or by creating one). This enables one-off, manual debugging on production (e.g., update 1–2 rows, inspect replication/table schemas) without modifying the target Postgres and while relying on existing infra trust (k8s port-forward + Tailscale).
Trust model: No per-user DB auth; infra gating (port-forward + Tailscale) is the boundary. Optionally serve server-TLS; no client certs.

0) Assumptions & scope
You already know where to connect upstream and can obtain a pgx.Conn (or *pgconn.PgConn) per client session using infra creds/net (K8s PF, Tailscale, Privatelink, SSH, etc.).
Supported: simple Query messages, multi-statement SQL (semicolon-separated), \d-style catalog queries, DML like UPDATE/INSERT/DELETE.
Rejected (clear error): extended protocol (Parse/Bind/Execute/Sync), COPY, replication protocol, GSS/SASL auth.
TLS to clients: either plaintext (rely on Tailscale) or server-TLS (no client certs).
Identity: none—infra is the gate.
1) High-level architecture
Per client TCP session → one upstream Postgres connection.
psql ──pgwire──> [Go proxy]
                  │
                  ├─ open upstream pgx.Conn (your creds)
                  └─ loop: Query → upstream SimpleQuery → stream results back
Core pieces
listener: accepts client TCP; optional TLS.
session: handles pgwire handshake + message loop.
upstream: creates and holds a pgx.Conn (or *pgconn.PgConn) tied to the session.
codec: minimal pgwire encode/decode using github.com/jackc/pgproto3/v2.
guardrails: timeouts, max rows/bytes, denylist of statements you don’t want to allow.
cancel: map BackendKeyData and implement CancelRequest.
2) Protocol surface you must implement (server side)
SSL negotiation
If you don’t want TLS: on SSLRequest respond with a single byte 'N'.
If you want server-only TLS: respond 'S' and switch to tls.Server(...).
StartupMessage
Parse parameters (user, database, application_name, etc.). You don’t need them for auth; just record for logging.
Send the minimal startup sequence:
AuthenticationOk
ParameterStatus (at least):
server_version, server_encoding=UTF8, client_encoding=UTF8, DateStyle=ISO, MDY, TimeZone=UTC, standard_conforming_strings=on
BackendKeyData (pid, secret) from the upstream connection (so cancels work)
ReadyForQuery('I')
Simple protocol loop
Accept Query messages (Q), extract SQL string.
Execute upstream with simple query (PgConn.SimpleQuery(sql)), and for each result:
Emit RowDescription (if there are fields)
Emit DataRow per row
Emit CommandComplete with the upstream tag (e.g., UPDATE 1)
After each message batch: ReadyForQuery('I')
On Terminate: close.
CancelRequest
When a new connection arrives with CancelRequest (different protocol code), open a new short-lived upstream TCP connection and write a CancelRequest using the same pid/secret you gave the client. Close.
Errors
Convert upstream *pgconn.PgError to an ErrorResponse with fields: Severity, Code, Message, Detail/Hint/Position if present. Then ReadyForQuery('E'→ back to 'I').
Everything else: return ErrorResponse("unsupported message (use simple protocol)").
3) Concurrency & resource model
One goroutine per client session (plus one for upstream pgx.Conn if you want heartbeats).
Use a context with deadline per query.
Limits:
statement_timeout (server-side via SET on upstream) and also a proxy-side query timeout.
Cap max returned rows/bytes; stop after the cap with a friendly error to avoid megadumps during incidents.
4) Upstream connection bring-up (pgx/pgconn)
Create the upstream connection before you finish client startup so you can forward real BackendKeyData.
Example (conceptual):
cfg, _ := pgx.ParseConfig(dsn)                 // your infra creds
cfg.RuntimeParams = map[string]string{
  "application_name": "go-proxy/tsnode=X",     // audit trail
  "statement_timeout": "30s",
  "idle_in_transaction_session_timeout": "30s",
  "standard_conforming_strings": "on",
}
conn, err := pgx.ConnectConfig(ctx, cfg)       // 1 upstream per client
pg := conn.PgConn()
pid := pg.PID()
key := pg.SecretKey()
Keep pid/key to send in BackendKeyData to the client and for future cancels.
5) Message flow (happy path)
Client connects → optionally TLS handshake.
Client sends SSLRequest → you reply 'N' (or TLS).
Client sends StartupMessage → you:
open upstream pgx
send: AuthenticationOk, ParameterStatus*, BackendKeyData(pid,key), ReadyForQuery('I')
Loop:
Receive Query(sql)
pg.PgConn().SimpleQuery(ctx, sql) → returns []*pgconn.Result
For each result:
If it has fields: write RowDescription(result.FieldDescriptions)
For each row: DataRow(values...)
Write CommandComplete(result.CommandTag)
Write ReadyForQuery('I')
On Terminate → close upstream and client.
6) Key implementation details
6.1 pgwire read/write with pgproto3
Read from client with a Frontend:
fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
msg, err := fe.Receive()
// handle *pgproto3.StartupMessage, *pgproto3.Query, *pgproto3.Terminate, *pgproto3.CancelRequest
Write to client by encoding message types directly to the raw io.Writer (clientConn):
_, _ = clientConn.Write((&pgproto3.AuthenticationOk{}).Encode(nil))
// ... ParameterStatus, BackendKeyData, ReadyForQuery
6.2 Streaming results back
Use pgconn’s simple query path to get wire-accurate field metadata:
results, err := pg.PgConn().SimpleQuery(ctx, sql) // []*pgconn.Result
for _, r := range results {
    if f := r.FieldDescriptions; len(f) > 0 {
        // f is []pgproto3.FieldDescription; you can send it back as-is
        _, _ = clientConn.Write((&pgproto3.RowDescription{Fields: f}).Encode(nil))
        for _, row := range r.Rows {                  // [][]byte slices
            dr := &pgproto3.DataRow{Values: row}      // text format
            _, _ = clientConn.Write(dr.Encode(nil))
        }
    }
    _, _ = clientConn.Write((&pgproto3.CommandComplete{CommandTag: r.CommandTag}).Encode(nil))
}
_, _ = clientConn.Write((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil))
This avoids doing your own OID/type mapping—the server already did it, and FieldDescriptions are exact.
6.3 Cancel handling
Store pid/secret per session.
If a new connection arrives and its first message decodes as *pgproto3.CancelRequest, open a raw TCP/TLS connection to the upstream Postgres address and write a CancelRequest constructed with those same values:
// Build packet: int32 length + int32 cancel code(80877102) + int32 pid + int32 secret
// Send, then close.
If you can’t map the pid/secret (unknown session), just drop it.
6.4 Error translation
Catch upstream errors:
var pgErr *pgconn.PgError
if errors.As(err, &pgErr) {
  er := &pgproto3.ErrorResponse{
    Severity: "ERROR", Code: pgErr.Code, Message: pgErr.Message,
    Detail: pgErr.Detail, Hint: pgErr.Hint,
    Position: pgErr.Position, SchemaName: pgErr.SchemaName,
    TableName: pgErr.TableName, ColumnName: pgErr.ColumnName,
  }
  _, _ = clientConn.Write(er.Encode(nil))
  _, _ = clientConn.Write((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil))
  return
}
6.5 Guardrails (strongly recommended)
Before executing, fast-fail anything you forbid:
(Optional) denylist: ALTER, DROP, TRUNCATE, VACUUM, REINDEX, COPY, CALL. Allow UPDATE/INSERT/DELETE if needed.
Proxy-side row/byte caps:
Track emitted rows and bytes per query; abort gracefully if thresholds exceeded.
Timeouts:
Upstream SET LOCAL statement_timeout='30s'.
Context deadline on SimpleQuery.
Transaction handling:
Let users run BEGIN/COMMIT/ROLLBACK—you’re relaying on a single upstream conn, so it just works.
Always end each cycle with ReadyForQuery reflecting tx state ('I', 'T', 'E'). If you only ever claim 'I', block multi-statement transactions or track status from upstream.
7) TLS modes
Plain (fastest): reply 'N' to SSLRequest; require sslmode=disable client-side. Safe if you’re strictly on Tailscale/port-forward paths.
Server-only TLS: reply 'S', then wrap with tls.Server(...). No client certs; psql can do sslmode=verify-full with your CA.
8) Observability
Set upstream application_name='go-proxy tsnode=<device>|user=<who>' for audit.
Structured logs per query: duration, rows, bytes, tag, truncated, err.code.
Emit log_line_prefix-like details on proxy side too.
9) Testing matrix
psql basics: SELECT 1, multi-statement SELECT 1; SELECT 2;.
\d, \dt, \d+ schema.table.
DML: UPDATE ... WHERE pk = ...; with RETURNING.
Large result set truncation.
Error surfaces: syntax error, permission error, timeout.
Cancel: run pg_sleep(10) then cancel with Ctrl+C.
Transactions: BEGIN; UPDATE ...; ROLLBACK;
10) Minimal skeleton (abridged)
This is intentionally compact to show the glue points; fill in checks, errors, and guards.
type Session struct {
  cli    net.Conn              // client (psql)
  up     *pgx.Conn             // upstream
  pg     *pgconn.PgConn        // upstream low-level
  pid    uint32
  secret uint32
}

func (s *Session) handle() error {
  fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(s.cli), s.cli)

  // 1) SSLRequest?
  if msg, _ := fe.Receive(); m, ok := msg.(*pgproto3.SSLRequest); ok {
    s.cli.Write([]byte("N")) // or do TLS here
    // read next message
  }

  // 2) Startup
  msg, err := fe.Receive()
  sm := msg.(*pgproto3.StartupMessage)

  // open upstream now (your DSN/creds)
  up, _ := pgx.Connect(ctx, os.Getenv("UPSTREAM_DSN"))
  s.up = up; s.pg = up.PgConn()
  s.pid = s.pg.PID(); s.secret = s.pg.SecretKey()

  // send server greeting
  w := s.cli
  w.Write((&pgproto3.AuthenticationOk{}).Encode(nil))
  for k, v := range map[string]string{
    "server_version":"15.4", "server_encoding":"UTF8",
    "client_encoding":"UTF8", "DateStyle":"ISO, MDY",
    "TimeZone":"UTC", "standard_conforming_strings":"on",
  } {
    w.Write((&pgproto3.ParameterStatus{Name:k, Value:v}).Encode(nil))
  }
  w.Write((&pgproto3.BackendKeyData{ProcessID:int32(s.pid), SecretKey:int32(s.secret)}).Encode(nil))
  w.Write((&pgproto3.ReadyForQuery{TxStatus:'I'}).Encode(nil))

  // 3) Loop
  for {
    msg, err := fe.Receive()
    switch m := msg.(type) {
    case *pgproto3.Query:
      ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
      results, err := s.pg.SimpleQuery(ctx, m.String)
      cancel()
      if err != nil { s.sendPgError(err); continue }
      for _, r := range results {
        if len(r.FieldDescriptions) > 0 {
          w.Write((&pgproto3.RowDescription{Fields:r.FieldDescriptions}).Encode(nil))
          for _, row := range r.Rows {
            w.Write((&pgproto3.DataRow{Values:row}).Encode(nil))
          }
        }
        w.Write((&pgproto3.CommandComplete{CommandTag:r.CommandTag}).Encode(nil))
      }
      w.Write((&pgproto3.ReadyForQuery{TxStatus:'I'}).Encode(nil))
    case *pgproto3.Terminate:
      return nil
    default:
      w.Write((&pgproto3.ErrorResponse{Severity:"ERROR", Code:"0A000", Message:"unsupported message"}).Encode(nil))
      w.Write((&pgproto3.ReadyForQuery{TxStatus:'I'}).Encode(nil))
    }
  }
}

func (s *Session) sendPgError(err error) {
  var pgErr *pgconn.PgError
  if errors.As(err, &pgErr) {
    s.cli.Write((&pgproto3.ErrorResponse{
      Severity: "ERROR", Code: pgErr.Code, Message: pgErr.Message,
      Detail: pgErr.Detail, Hint: pgErr.Hint,
    }).Encode(nil))
  } else {
    s.cli.Write((&pgproto3.ErrorResponse{Severity:"ERROR", Code:"XX000", Message:err.Error()}).Encode(nil))
  }
  s.cli.Write((&pgproto3.ReadyForQuery{TxStatus:'I'}).Encode(nil))
}
11) Hardening checklist (for your prod use case)
Deny dangerous statements unless you explicitly allow them; keep a short allowlist if you prefer (e.g., SELECT, SHOW, UPDATE ... WHERE pk).
Row/byte caps with explicit override flag if you need to lift them during an incident.
Statement timeout both proxy-side and upstream (GUC).
Logging: include the client source (tailscale node), upstream addr, duration, tag, error code.
Backpressure: use SetReadDeadline/SetWriteDeadline; respect client disconnects.
Graceful shutdown: stop accepting, drain sessions with a deadline.
