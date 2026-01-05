package pgwire

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	connmongo "github.com/PeerDB-io/peerdb/flow/connectors/mongo"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pgwire/mongosh"
	"github.com/PeerDB-io/peerdb/flow/pgwire/mongosh/command"
)

// wrapMongoError converts a MongoDB error to an UpstreamError
func wrapMongoError(err error) error {
	if err == nil {
		return nil
	}
	return &UpstreamError{Resp: &pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	}}
}

// MongoUpstream implements Upstream for MongoDB databases
type MongoUpstream struct {
	conn       *connmongo.MongoConnector
	database   string
	commentTag string // Unique tag for identifying operations (used for cancel)
	secret     uint32 // Secret for cancel routing (pid is always 0)
}

// NewMongoUpstream creates a new MongoDB upstream connection
func NewMongoUpstream(ctx context.Context, config *protos.MongoConfig, database string) (*MongoUpstream, error) {
	conn, err := connmongo.NewMongoConnector(ctx, config)
	if err != nil {
		return nil, err
	}

	// Generate secret for cancel routing and unique comment tag
	secret := rand.Uint32() //nolint:gosec // not security-critical, used for cancel routing
	commentTag := fmt.Sprintf("peerdb-%08x", secret)

	return &MongoUpstream{
		conn:       conn,
		database:   database,
		commentTag: commentTag,
		secret:     secret,
	}, nil
}

// Exec executes a query and returns results for streaming
func (u *MongoUpstream) Exec(ctx context.Context, query string) (ResultIterator, error) {
	spec, err := mongosh.Compile(query)
	if err != nil {
		return nil, wrapMongoError(err)
	}

	// Handle help requests - return one row per line for better display in psql
	if spec.HelpText != "" {
		return NewMongoHelpIterator(spec.HelpText), nil
	}

	// Add comment for cancel support only on commands that support it
	cmd := spec.Command
	if len(cmd) > 0 && command.CommandSupportsComment(cmd[0].Key) {
		cmd = slices.Concat(cmd, bson.D{{Key: "comment", Value: u.commentTag}})
	}

	// Use admin database for commands that require it (e.g., listDatabases), otherwise use connection database
	database := u.database
	if spec.AdminDB {
		database = "admin"
	}
	db := u.conn.Client().Database(database)

	switch spec.ResultKind {
	case mongosh.ResultCursor:
		cursor, err := db.RunCommandCursor(ctx, cmd)
		if err != nil {
			return nil, wrapMongoError(err)
		}
		return &MongoCursorIterator{cursor: cursor, consumed: false}, nil

	default: // ResultScalar
		result := db.RunCommand(ctx, cmd)
		if err := result.Err(); err != nil {
			return nil, wrapMongoError(err)
		}
		// Decode the result document
		var doc bson.D
		if err := result.Decode(&doc); err != nil {
			return nil, wrapMongoError(err)
		}
		return &MongoScalarIterator{doc: doc, consumed: false}, nil
	}
}

// TxStatus returns the transaction status - always 'I' (idle) for MongoDB
func (u *MongoUpstream) TxStatus() byte {
	return 'I'
}

// ServerParameters returns fake PostgreSQL parameters for client compatibility
func (u *MongoUpstream) ServerParameters(ctx context.Context) map[string]string {
	return map[string]string{
		"server_version":              "16.0-mongodb-proxy",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}
}

// BackendKeyData returns (0, secret) for cancel routing
func (u *MongoUpstream) BackendKeyData() (uint32, uint32) {
	return 0, u.secret
}

// Cancel cancels running operations by finding them via comment and killing them
func (u *MongoUpstream) Cancel(ctx context.Context) error {
	// Use currentOp to find operations with our comment tag
	// $ownOps: true limits to operations from this client (requires auth)
	// We filter by command.comment to find our specific operations
	currentOpCmd := bson.D{
		{Key: "currentOp", Value: 1},
		{Key: "$ownOps", Value: true},
		{Key: "comment", Value: u.commentTag},
	}

	adminDB := u.conn.Client().Database("admin")
	result := adminDB.RunCommand(ctx, currentOpCmd)
	if err := result.Err(); err != nil {
		// If currentOp fails (e.g., no permission), silently ignore
		return nil
	}

	var currentOpResult struct {
		Inprog []struct {
			OpID any `bson:"opid"` // Can be int32, int64, or string depending on MongoDB version
		} `bson:"inprog"`
	}
	if err := result.Decode(&currentOpResult); err != nil {
		return nil
	}

	// Kill each matching operation
	for _, op := range currentOpResult.Inprog {
		if op.OpID == nil {
			continue
		}
		killCmd := bson.D{
			{Key: "killOp", Value: 1},
			{Key: "op", Value: op.OpID},
		}
		// Best effort - ignore errors from killOp
		_ = adminDB.RunCommand(ctx, killCmd)
	}

	return nil
}

// Close closes the upstream connection
func (u *MongoUpstream) Close() error {
	if u.conn != nil {
		return u.conn.Close()
	}
	return nil
}

// CheckQuery validates a query by attempting to compile it
func (u *MongoUpstream) CheckQuery(query string) error {
	_, err := mongosh.Compile(query)
	return err
}

// jsonFieldDescription is the single column returned for all MongoDB results
var jsonFieldDescription = []FieldDescription{{
	Name:        "result",
	DataTypeOID: 114, // JSON OID
	Format:      0,   // Text format
}}

// MongoCursorIterator implements ResultIterator for cursor results
//
//nolint:govet // fieldalignment: readability preferred
type MongoCursorIterator struct {
	cursor   *mongo.Cursor
	consumed bool
	current  bson.D
	rowCount int64
	err      error
}

// NextResult advances to the next result set (only one for MongoDB)
func (it *MongoCursorIterator) NextResult() bool {
	if it.consumed {
		return false
	}
	it.consumed = true
	return true
}

// FieldDescriptions returns the single JSON column
func (it *MongoCursorIterator) FieldDescriptions() []FieldDescription {
	return jsonFieldDescription
}

// NextRow advances to the next document
func (it *MongoCursorIterator) NextRow() bool {
	if it.cursor == nil {
		return false
	}
	if it.cursor.Next(context.Background()) {
		it.rowCount++
		if err := it.cursor.Decode(&it.current); err != nil {
			it.err = err
			return false
		}
		return true
	}
	it.err = it.cursor.Err()
	return false
}

// RowValues returns the current document as Extended JSON
func (it *MongoCursorIterator) RowValues() [][]byte {
	if it.current == nil {
		return nil
	}
	jsonBytes, err := bson.MarshalExtJSON(it.current, true, false)
	if err != nil {
		it.err = err
		return nil
	}
	return [][]byte{jsonBytes}
}

// CommandTag returns the command completion tag
func (it *MongoCursorIterator) CommandTag() string {
	return fmt.Sprintf("SELECT %d", it.rowCount)
}

// Err returns any error encountered
func (it *MongoCursorIterator) Err() error {
	return it.err
}

// Close releases resources for current result set
func (it *MongoCursorIterator) Close() {
	// Cursor closed in CloseAll
}

// CloseAll closes the cursor
func (it *MongoCursorIterator) CloseAll() error {
	if it.cursor != nil {
		return it.cursor.Close(context.Background())
	}
	return nil
}

// MongoScalarIterator implements ResultIterator for scalar (single document) results
type MongoScalarIterator struct {
	doc         bson.D
	consumed    bool
	rowReturned bool
}

// NextResult advances to the next result set (only one for scalar)
func (it *MongoScalarIterator) NextResult() bool {
	if it.consumed {
		return false
	}
	it.consumed = true
	return true
}

// FieldDescriptions returns the single JSON column
func (it *MongoScalarIterator) FieldDescriptions() []FieldDescription {
	return jsonFieldDescription
}

// NextRow returns true once for the single document
func (it *MongoScalarIterator) NextRow() bool {
	if it.rowReturned {
		return false
	}
	it.rowReturned = true
	return true
}

// RowValues returns the document as Extended JSON
func (it *MongoScalarIterator) RowValues() [][]byte {
	if it.doc == nil {
		return nil
	}
	jsonBytes, err := bson.MarshalExtJSON(it.doc, true, false)
	if err != nil {
		return nil
	}
	return [][]byte{jsonBytes}
}

// CommandTag returns "OK" for scalar results
func (it *MongoScalarIterator) CommandTag() string {
	return "OK"
}

// Err returns nil (no streaming errors for scalar)
func (it *MongoScalarIterator) Err() error {
	return nil
}

// Close is a no-op for scalar results
func (it *MongoScalarIterator) Close() {}

// CloseAll is a no-op for scalar results
func (it *MongoScalarIterator) CloseAll() error {
	return nil
}

// textFieldDescription is used for help output (plain text, not JSON)
var textFieldDescription = []FieldDescription{{
	Name:        "help",
	DataTypeOID: 25, // TEXT OID
	Format:      0,  // Text format
}}

// MongoHelpIterator implements ResultIterator for help text (one row per line)
type MongoHelpIterator struct {
	lines    []string
	consumed bool
	index    int
}

// NewMongoHelpIterator creates a help iterator from help text
func NewMongoHelpIterator(helpText string) *MongoHelpIterator {
	lines := strings.Split(strings.TrimSuffix(helpText, "\n"), "\n")
	return &MongoHelpIterator{lines: lines}
}

// NextResult advances to the next result set (only one for help)
func (it *MongoHelpIterator) NextResult() bool {
	if it.consumed {
		return false
	}
	it.consumed = true
	return true
}

// FieldDescriptions returns the single text column
func (it *MongoHelpIterator) FieldDescriptions() []FieldDescription {
	return textFieldDescription
}

// NextRow advances to the next line
func (it *MongoHelpIterator) NextRow() bool {
	if it.index >= len(it.lines) {
		return false
	}
	it.index++
	return true
}

// RowValues returns the current line as text
func (it *MongoHelpIterator) RowValues() [][]byte {
	if it.index == 0 || it.index > len(it.lines) {
		return nil
	}
	return [][]byte{[]byte(it.lines[it.index-1])}
}

// CommandTag returns the row count
func (it *MongoHelpIterator) CommandTag() string {
	return fmt.Sprintf("SELECT %d", len(it.lines))
}

// Err returns nil (no errors for help)
func (it *MongoHelpIterator) Err() error {
	return nil
}

// Close is a no-op for help results
func (it *MongoHelpIterator) Close() {}

// CloseAll is a no-op for help results
func (it *MongoHelpIterator) CloseAll() error {
	return nil
}
