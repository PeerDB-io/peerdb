# MongoDB Backend Support Requirements

## Context
Add MongoDB backend support to the existing PeerDB pgwire proxy while retaining full PostgreSQL proxy functionality. Users can run both backend types simultaneously.

## Product Requirements

### 1. Multi-Backend Support
- Support both PostgreSQL and MongoDB backends
- Run different backends on different ports simultaneously
- Backend type configured at server startup (not per-connection)
- PostgreSQL remains the default backend

### 2. MongoDB Query Interface
- Accept MongoDB queries in native MongoDB query language (JSON format)
- No SQL translation - users write MongoDB queries directly
- Examples: `db.users.find({})`, `db.runCommand({})`

### 3. Result Format
- Return all MongoDB results as JSON text in a single column
- Column appears as standard PostgreSQL JSON type to clients
- All BSON types converted to JSON-compatible representations

### 4. Client Compatibility
- Works with any PostgreSQL client (psql, pgcli, etc.)
- No client-side changes required
- Maintains full PostgreSQL wire protocol compatibility
- Cancel/interrupt support (Ctrl+C) works for both backends

### 5. Configuration & Deployment
- Configure backend type via open server port
- MongoDB connection string separate from PostgreSQL DSN
- Can run multiple proxy instances with different backends
- All existing security controls (timeouts, row limits, etc.) apply to both backends

## Basic Architecture Decisions

### Backend Abstraction
- Abstract backend operations behind a common interface
- Session uses single backend type for its lifetime
- No mixing of backends within a session

### Connection Model
- One backend connection per client session (same as current PostgreSQL model)
- MongoDB connections managed similarly to PostgreSQL connections

### Error Handling
- MongoDB errors wrapped in PostgreSQL wire protocol format
- Error messages preserve full MongoDB error information (including error codes and details that mongosh would show)
- Simple SQLSTATE mapping: use generic code (XX000)
- psql displays the complete MongoDB error message string

## Use Cases
- Debug MongoDB CDC pipelines using familiar PostgreSQL tools
- Query MongoDB during incidents without installing mongosh
