package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// SqlSelectPgCatalogRe detects psql \d \dt \dt+ commands which query pg_catalog
var SqlSelectPgCatalogRe = regexp.MustCompile(`(?is)^\s*SELECT\b.*\bpg_catalog\b`)

// Upstream abstracts database connections for the pgwire proxy
type Upstream interface {
	// Exec executes a query and returns results for streaming to client
	Exec(ctx context.Context, query string) (ResultIterator, error)

	// TxStatus returns the transaction status byte ('I', 'T', or 'E')
	TxStatus() byte

	// ServerParameters returns parameters to send in the startup greeting
	ServerParameters(ctx context.Context) map[string]string

	// BackendKeyData returns (pid, secret) for cancel request routing
	BackendKeyData() (uint32, uint32)

	// Cancel cancels the currently running query
	Cancel(ctx context.Context) error

	// Close closes the upstream connection
	Close() error

	// CheckQuery validates a query against security rules (blocked commands, bypass attempts)
	// This is database-specific as SQL parsing differs between dialects
	CheckQuery(query string) error
}

// ResultIterator streams query results
type ResultIterator interface {
	// NextResult advances to the next result set (for multi-statement queries)
	NextResult() bool

	// FieldDescriptions returns column metadata for current result
	FieldDescriptions() []FieldDescription

	// NextRow advances to next row, returns false when done
	NextRow() bool

	// RowValues returns current row's values as text-encoded bytes
	RowValues() [][]byte

	// CommandTag returns the command tag (e.g., "SELECT 5", "UPDATE 3")
	CommandTag() string

	// Err returns any error encountered during iteration
	Err() error

	// Close releases resources for current result set
	Close()

	// CloseAll closes entire multi-result and returns any final error
	CloseAll() error
}

// FieldDescription describes a column in a result set
type FieldDescription struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         int32
	Format               int16
}

// UpstreamError wraps a pgproto3.ErrorResponse for database-agnostic error handling
type UpstreamError struct {
	Resp *pgproto3.ErrorResponse
}

func (e *UpstreamError) Error() string {
	return e.Resp.Message
}

// NewUpstream creates an upstream connection based on peer configuration
func NewUpstream(ctx context.Context, catalogPool shared.CatalogPool, peerName string, queryTimeout time.Duration) (Upstream, error) {
	if peerName == "" {
		return nil, errors.New("database name (peer name) is required")
	}

	// Special case: "catalog" connects to the PeerDB catalog database
	if peerName == "catalog" {
		pgConfig := internal.GetCatalogPostgresConfigFromEnv(ctx)
		return NewPostgresUpstream(ctx, pgConfig, queryTimeout)
	}

	// Load peer from catalog
	peer, err := connectors.LoadPeer(ctx, catalogPool, peerName)
	if err != nil {
		return nil, fmt.Errorf("peer '%s' not found", peerName)
	}

	switch peer.Type {
	case protos.DBType_POSTGRES:
		pgConfig := peer.GetPostgresConfig()
		if pgConfig == nil {
			return nil, fmt.Errorf("peer '%s' has no PostgreSQL configuration", peerName)
		}
		return NewPostgresUpstream(ctx, pgConfig, queryTimeout)

	case protos.DBType_MYSQL:
		mysqlConfig := peer.GetMysqlConfig()
		if mysqlConfig == nil {
			return nil, fmt.Errorf("peer '%s' has no MySQL configuration", peerName)
		}
		return NewMySQLUpstream(ctx, mysqlConfig, queryTimeout)

	case protos.DBType_MONGO:
		mongoConfig := peer.GetMongoConfig()
		if mongoConfig == nil {
			return nil, fmt.Errorf("peer '%s' has no MongoDB configuration", peerName)
		}
		cs, err := connstring.Parse(mongoConfig.Uri)
		if err != nil {
			return nil, fmt.Errorf("peer '%s' has invalid MongoDB URI: %w", peerName, err)
		}
		database := cs.Database
		if database == "" {
			database = "test" // match mongosh behavior
		}
		return NewMongoUpstream(ctx, mongoConfig, database)

	default:
		return nil, fmt.Errorf("peer '%s' is type %s, only PostgreSQL, MySQL, and MongoDB are supported", peerName, peer.Type)
	}
}
