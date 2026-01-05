package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PgwireMongoSuite struct {
	t      *testing.T
	source *MongoSource
	peer   *protos.Peer
	suffix string
}

func (s PgwireMongoSuite) T() *testing.T {
	return s.t
}

func (s PgwireMongoSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
}

func SetupPgwireMongoSuite(t *testing.T) PgwireMongoSuite {
	t.Helper()

	suffix := "pgwmo_" + strings.ToLower(shared.RandomString(8))
	source, err := SetupMongo(t, suffix)
	if err != nil {
		t.Skipf("MongoDB setup failed: %v", err)
	}

	// Create test database name
	testDb := GetTestDatabase(suffix)

	// Create peer with unique name - append database to URI
	peerConfig := &protos.MongoConfig{
		Uri:        source.config.Uri + "/" + testDb,
		Username:   source.config.Username,
		Password:   source.config.Password,
		DisableTls: source.config.DisableTls,
	}
	peer := &protos.Peer{
		Name: "mongo_" + suffix,
		Type: protos.DBType_MONGO,
		Config: &protos.Peer_MongoConfig{
			MongoConfig: peerConfig,
		},
	}
	CreatePeer(t, peer)

	// Create test collection with sample documents
	collection := source.AdminClient().Database(testDb).Collection("test_collection")
	docs := []any{
		bson.D{
			{Key: "_id", Value: 1},
			{Key: "name", Value: "alice"},
			{Key: "age", Value: 30},
			{Key: "active", Value: true},
		},
		bson.D{
			{Key: "_id", Value: 2},
			{Key: "name", Value: "bob"},
			{Key: "age", Value: 25},
			{Key: "active", Value: false},
		},
		bson.D{
			{Key: "_id", Value: 3},
			{Key: "name", Value: "charlie"},
			{Key: "age", Value: 35},
			{Key: "active", Value: true},
		},
	}
	_, err = collection.InsertMany(t.Context(), docs)
	require.NoError(t, err, "failed to insert test documents")

	// Verify pgwire proxy is available
	conn, err := pgx.Connect(t.Context(), pgwireDSN(peer.Name, nil))
	if err != nil {
		source.Teardown(t, t.Context(), suffix)
		t.Skipf("PgWire proxy not available: %v", err)
	}
	conn.Close(t.Context())

	return PgwireMongoSuite{
		t:      t,
		source: source,
		peer:   peer,
		suffix: suffix,
	}
}

func TestPgwireMongo(t *testing.T) {
	e2eshared.RunSuite(t, SetupPgwireMongoSuite)
}

// psql executes a mongosh query via psql and returns tuples-only output
func (s PgwireMongoSuite) psql(query string) (string, error) {
	return runPsql(s.t, s.peer.Name, "-tA", "-c", query)
}

// testCollection returns the test collection name
func (s PgwireMongoSuite) testCollection() string {
	return "test_collection"
}

// testDatabase returns the test database name
func (s PgwireMongoSuite) testDatabase() string {
	return GetTestDatabase(s.suffix)
}

// queryWithOptions executes a query with custom connection options
func (s PgwireMongoSuite) queryWithOptions(query string, options map[string]string) error {
	dsn := pgwireDSN(s.peer.Name, options)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	if err != nil {
		return err
	}
	defer conn.Close(s.t.Context())

	rows, err := conn.Query(s.t.Context(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// drain rows
	}
	return rows.Err()
}

// ========================================
// Basic Connectivity Tests
// ========================================

func (s PgwireMongoSuite) Test_BasicConnectivity_Ping() {
	output, err := s.psql(`db.runCommand({ping: 1})`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, `"ok"`)
}

func (s PgwireMongoSuite) Test_BasicConnectivity_ShowCollections() {
	output, err := s.psql(`show collections`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "test_collection")
}

func (s PgwireMongoSuite) Test_BasicConnectivity_ShowDatabases() {
	output, err := s.psql(`show databases`)
	require.NoError(s.t, err)
	// Should contain at least admin or the test database
	require.NotEmpty(s.t, output)
}

// ========================================
// Find Operations Tests
// ========================================

func (s PgwireMongoSuite) Test_Find_AllDocuments() {
	output, err := s.psql(fmt.Sprintf(`db.%s.find({})`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "bob")
	require.Contains(s.t, output, "charlie")
}

func (s PgwireMongoSuite) Test_Find_WithFilter() {
	output, err := s.psql(fmt.Sprintf(`db.%s.find({active: true})`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "charlie")
	require.NotContains(s.t, output, "bob")
}

func (s PgwireMongoSuite) Test_Find_WithProjection() {
	output, err := s.psql(fmt.Sprintf(`db.%s.find({}, {_id: 0, name: 1})`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "name")
	// Should not contain age field
	require.NotContains(s.t, output, `"age"`)
}

func (s PgwireMongoSuite) Test_FindOne_Basic() {
	output, err := s.psql(fmt.Sprintf(`db.%s.findOne({name: "alice"})`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "30")
}

func (s PgwireMongoSuite) Test_Find_WithChainers() {
	s.t.Run("Sort", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({}).sort({age: -1})`, s.testCollection()))
		require.NoError(t, err)
		// charlie (35) should appear before alice (30) who appears before bob (25)
		charlieIdx := strings.Index(output, "charlie")
		aliceIdx := strings.Index(output, "alice")
		bobIdx := strings.Index(output, "bob")
		require.True(t, charlieIdx < aliceIdx && aliceIdx < bobIdx, "Documents should be sorted by age descending")
	})

	s.t.Run("Limit", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({}).limit(1)`, s.testCollection()))
		require.NoError(t, err)
		// Count occurrences - should only have one document
		count := strings.Count(output, `"_id"`)
		require.Equal(t, 1, count, "Should return only 1 document")
	})

	s.t.Run("Skip", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({}).skip(1).limit(1)`, s.testCollection()))
		require.NoError(t, err)
		require.NotEmpty(t, output)
	})

	s.t.Run("SortAndLimit", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({}).sort({age: 1}).limit(1)`, s.testCollection()))
		require.NoError(t, err)
		// bob has lowest age (25)
		require.Contains(t, output, "bob")
	})
}

// ========================================
// Data Types Tests
// ========================================

func (s PgwireMongoSuite) Test_DataTypes() {
	// Insert documents with various data types
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("types_test")
	_, err := collection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: "types_doc"},
		{Key: "string_field", Value: "hello"},
		{Key: "int_field", Value: 42},
		{Key: "float_field", Value: 3.14},
		{Key: "bool_field", Value: true},
		{Key: "null_field", Value: nil},
		{Key: "array_field", Value: bson.A{1, 2, 3}},
		{Key: "nested_field", Value: bson.D{{Key: "inner", Value: "value"}}},
	})
	require.NoError(s.t, err)

	tests := []struct {
		name     string
		field    string
		expected string
	}{
		{"String", "string_field", "hello"},
		{"Integer", "int_field", `"$numberInt":"42"`},
		{"Float", "float_field", `"$numberDouble":"3.14"`},
		{"Boolean", "bool_field", "true"},
		{"Array", "array_field", `"$numberInt":"1"`},  // Extended JSON array element
		{"Nested", "nested_field", `"inner":"value"`}, // Nested object field
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(`db.types_test.findOne({_id: "types_doc"})`)
			require.NoError(t, err)
			require.Contains(t, output, tt.expected)
		})
	}
}

// ========================================
// Aggregation Tests
// ========================================

func (s PgwireMongoSuite) Test_Aggregate_Match() {
	output, err := s.psql(fmt.Sprintf(`db.%s.aggregate([{$match: {active: true}}])`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "charlie")
	require.NotContains(s.t, output, "bob")
}

func (s PgwireMongoSuite) Test_Aggregate_Group() {
	output, err := s.psql(fmt.Sprintf(`db.%s.aggregate([{$group: {_id: "$active", count: {$sum: 1}}}])`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "count")
}

func (s PgwireMongoSuite) Test_Aggregate_Pipeline() {
	output, err := s.psql(fmt.Sprintf(`db.%s.aggregate([
		{$match: {active: true}},
		{$project: {name: 1, _id: 0}},
		{$sort: {name: 1}}
	])`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "charlie")
}

// ========================================
// Query Operators Tests
// ========================================

func (s PgwireMongoSuite) Test_Operators_Comparison() {
	tests := []struct {
		name     string
		filter   string
		expected []string
		excluded []string
	}{
		{"GreaterThan", `{age: {$gt: 30}}`, []string{"charlie"}, []string{"alice", "bob"}},
		{"LessThan", `{age: {$lt: 30}}`, []string{"bob"}, []string{"alice", "charlie"}},
		{"GreaterThanOrEqual", `{age: {$gte: 30}}`, []string{"alice", "charlie"}, []string{"bob"}},
		{"In", `{name: {$in: ["alice", "bob"]}}`, []string{"alice", "bob"}, []string{"charlie"}},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			output, err := s.psql(fmt.Sprintf(`db.%s.find(%s)`, s.testCollection(), tt.filter))
			require.NoError(t, err)
			for _, exp := range tt.expected {
				require.Contains(t, output, exp)
			}
			for _, exc := range tt.excluded {
				require.NotContains(t, output, exc)
			}
		})
	}
}

func (s PgwireMongoSuite) Test_Operators_Logical() {
	s.t.Run("And", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({$and: [{active: true}, {age: {$gt: 30}}]})`, s.testCollection()))
		require.NoError(t, err)
		require.Contains(t, output, "charlie")
		require.NotContains(t, output, "alice")
	})

	s.t.Run("Or", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({$or: [{name: "alice"}, {name: "bob"}]})`, s.testCollection()))
		require.NoError(t, err)
		require.Contains(t, output, "alice")
		require.Contains(t, output, "bob")
		require.NotContains(t, output, "charlie")
	})
}

// ========================================
// Error Handling Tests
// ========================================

func (s PgwireMongoSuite) Test_Error_SyntaxError() {
	_, err := s.psql(`db.coll.find({invalid syntax`)
	require.Error(s.t, err)
}

func (s PgwireMongoSuite) Test_Error_EmptyCollection() {
	// Querying non-existent collection should return empty result, not error
	output, err := s.psql(`db.nonexistent_collection.find({})`)
	require.NoError(s.t, err)
	// Should be empty or show no documents
	require.NotContains(s.t, output, "_id")
}

// ========================================
// Denied Operations Tests
// ========================================

func (s PgwireMongoSuite) Test_DeniedOperations() {
	tests := []struct {
		name  string
		query string
	}{
		{"InsertOne", `db.test.insertOne({a: 1})`},
		{"InsertMany", `db.test.insertMany([{a: 1}])`},
		{"UpdateOne", `db.test.updateOne({}, {$set: {a: 1}})`},
		{"UpdateMany", `db.test.updateMany({}, {$set: {a: 1}})`},
		{"DeleteOne", `db.test.deleteOne({})`},
		{"DeleteMany", `db.test.deleteMany({})`},
		{"Drop", `db.test.drop()`},
		{"DropDatabase", `db.dropDatabase()`},
		{"CreateCollection", `db.createCollection("new_coll")`},
		{"CreateIndex", `db.test.createIndex({a: 1})`},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			_, err := s.psql(tt.query)
			require.Error(t, err, "Operation %s should be denied", tt.name)
		})
	}
}

func (s PgwireMongoSuite) Test_DeniedChainers() {
	tests := []struct {
		name  string
		query string
	}{
		{"ToArray", fmt.Sprintf(`db.%s.find({}).toArray()`, s.testCollection())},
		{"ForEach", fmt.Sprintf(`db.%s.find({}).forEach(function(d){})`, s.testCollection())},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			_, err := s.psql(tt.query)
			require.Error(t, err, "Chainer %s should be denied", tt.name)
		})
	}
}

// ========================================
// Guardrails Tests
// ========================================

func (s PgwireMongoSuite) Test_Guardrails_MaxRows() {
	// Insert more documents for this test
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("guardrails_test")
	docs := make([]any, 20)
	for i := range 20 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "value", Value: i}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	// Query with row limit
	err = s.queryWithOptions(`db.guardrails_test.find({})`, map[string]string{"max_rows": "5"})
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "row limit")
}

func (s PgwireMongoSuite) Test_Guardrails_MaxBytes() {
	// Insert a document with large data
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("bytes_test")
	largeString := strings.Repeat("x", 1000)
	_, err := collection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: "large"},
		{Key: "data", Value: largeString},
	})
	require.NoError(s.t, err)

	// Query with byte limit
	err = s.queryWithOptions(`db.bytes_test.find({})`, map[string]string{"max_bytes": "100"})
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "byte limit")
}

func (s PgwireMongoSuite) Test_Guardrails_ResumeAfterRowLimit() {
	// Insert documents
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("resume_test")
	docs := make([]any, 10)
	for i := range 10 {
		docs[i] = bson.D{{Key: "_id", Value: i}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	dsn := pgwireDSN(s.peer.Name, map[string]string{"max_rows": "3"})
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// First query exceeds limit
	rows, err := conn.Query(s.t.Context(), `db.resume_test.find({})`)
	require.NoError(s.t, err, "Query should start successfully")

	for rows.Next() {
	}
	rowErr := rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to row limit")
	require.Contains(s.t, rowErr.Error(), "row limit")

	// Connection should still be usable
	var result string
	err = conn.QueryRow(s.t.Context(), `db.runCommand({ping: 1})`).Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after limit exceeded")
	require.Contains(s.t, result, "ok")
}

func (s PgwireMongoSuite) Test_Guardrails_ResumeAfterByteLimit() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("resume_bytes_test")
	largeString := strings.Repeat("y", 500)
	_, err := collection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: "large"},
		{Key: "data", Value: largeString},
	})
	require.NoError(s.t, err)

	dsn := pgwireDSN(s.peer.Name, map[string]string{"max_bytes": "500"})
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// First query exceeds byte limit
	rows, err := conn.Query(s.t.Context(), `db.resume_bytes_test.find({})`)
	require.NoError(s.t, err)

	for rows.Next() {
	}
	rowErr := rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to byte limit")
	require.Contains(s.t, rowErr.Error(), "byte limit")

	// Connection should still be usable
	var result string
	err = conn.QueryRow(s.t.Context(), `db.runCommand({ping: 1})`).Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after byte limit exceeded")
	require.Contains(s.t, result, "ok")
}

// ========================================
// Cancel Request Tests
// ========================================

func (s PgwireMongoSuite) Test_CancelRequest() {
	// Insert many documents for a slow query
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("cancel_test")
	docs := make([]any, 10000)
	for i := range 10000 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "data", Value: strings.Repeat("x", 100)}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	s.t.Run("CancelLongRunningQuery", func(t *testing.T) {
		dsn := pgwireDSN(s.peer.Name, nil)
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		errCh := make(chan error, 1)
		go func() {
			// Run a query that should take a while
			rows, err := conn.Query(s.t.Context(), `db.cancel_test.aggregate([{$sort: {data: 1}}, {$group: {_id: "$data"}}])`)
			if err != nil {
				errCh <- err
				return
			}
			for rows.Next() {
			}
			errCh <- rows.Err()
		}()

		time.Sleep(100 * time.Millisecond)

		err = conn.PgConn().CancelRequest(s.t.Context())
		require.NoError(t, err, "Cancel request should be sent")

		select {
		case queryErr := <-errCh:
			// Query may complete before cancel, that's ok
			if queryErr != nil {
				t.Logf("Query error (expected if canceled): %v", queryErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Query should have completed or been canceled within 10 seconds")
		}
	})
}

// ========================================
// Help Commands Tests
// ========================================

func (s PgwireMongoSuite) Test_Help() {
	s.t.Run("GlobalHelp", func(t *testing.T) {
		output, err := s.psql(`help()`)
		require.NoError(t, err)
		require.NotEmpty(t, output)
	})

	s.t.Run("DatabaseHelp", func(t *testing.T) {
		output, err := s.psql(`db.help()`)
		require.NoError(t, err)
		require.NotEmpty(t, output)
	})

	s.t.Run("CollectionHelp", func(t *testing.T) {
		output, err := s.psql(fmt.Sprintf(`db.%s.help()`, s.testCollection()))
		require.NoError(t, err)
		require.NotEmpty(t, output)
	})
}

// ========================================
// Large Results Tests
// ========================================

func (s PgwireMongoSuite) Test_LargeResults() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("large_test")
	docs := make([]any, 1000)
	for i := range 1000 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "name", Value: fmt.Sprintf("doc_%d", i)}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	output, err := s.psql(`db.large_test.find({})`)
	require.NoError(s.t, err)

	// Count documents returned
	count := strings.Count(output, `"_id"`)
	require.Equal(s.t, 1000, count, "Should return all 1000 documents")
}

// ========================================
// Misc Commands Tests
// ========================================

func (s PgwireMongoSuite) Test_CountDocuments() {
	output, err := s.psql(fmt.Sprintf(`db.%s.countDocuments({})`, s.testCollection()))
	require.NoError(s.t, err)
	// Parse the result to check count
	var result map[string]any
	err = json.Unmarshal([]byte(output), &result)
	require.NoError(s.t, err)
	// countDocuments uses aggregation, result should contain "n": 3
	if n, ok := result["n"].(float64); ok {
		require.InDelta(s.t, 3, n, 0.001)
	}
}

func (s PgwireMongoSuite) Test_EstimatedDocumentCount() {
	output, err := s.psql(fmt.Sprintf(`db.%s.estimatedDocumentCount()`, s.testCollection()))
	require.NoError(s.t, err)
	require.NotEmpty(s.t, output)
}

func (s PgwireMongoSuite) Test_GetIndexes() {
	output, err := s.psql(fmt.Sprintf(`db.%s.getIndexes()`, s.testCollection()))
	require.NoError(s.t, err)
	// Should at least have _id index
	require.Contains(s.t, output, "_id")
}

func (s PgwireMongoSuite) Test_Distinct() {
	output, err := s.psql(fmt.Sprintf(`db.%s.distinct("active")`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "true")
	require.Contains(s.t, output, "false")
}

// ========================================
// Empty Query Tests
// ========================================

func (s PgwireMongoSuite) Test_EmptyQuery() {
	tests := []struct {
		name string
		sql  string
	}{
		{"EmptySemicolon", ";"},
		{"MultipleSemicolons", ";;;"},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			dsn := pgwireDSN(s.peer.Name, nil)
			cfg, _ := pgx.ParseConfig(dsn)
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
			require.NoError(t, err)
			defer conn.Close(s.t.Context())

			// Empty queries should be handled as no-op by the proxy
			_, err = conn.Exec(s.t.Context(), tt.sql)
			require.NoError(t, err, "Empty query %q should succeed", tt.name)
		})
	}
}

// ========================================
// Error Handling and Recovery Tests
// ========================================

func (s PgwireMongoSuite) Test_ErrorRecovery() {
	s.t.Run("SyntaxErrorInQuery", func(t *testing.T) {
		// Syntax error should return an error
		_, err := s.psql(`db.coll.find({invalid syntax here`)
		require.Error(t, err, "Query with syntax error should fail")
	})

	s.t.Run("InvalidCommandError", func(t *testing.T) {
		// Invalid command should return an error
		_, err := s.psql(`db.runCommand({invalidCommand: 1})`)
		require.Error(t, err, "Invalid command should fail")
	})

	s.t.Run("ConnectionUsableAfterError", func(t *testing.T) {
		dsn := pgwireDSN(s.peer.Name, nil)
		cfg, _ := pgx.ParseConfig(dsn)
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
		require.NoError(t, err)
		defer conn.Close(s.t.Context())

		// First query fails with syntax error
		_, err = conn.Exec(s.t.Context(), `db.coll.find({invalid`)
		require.Error(t, err, "Syntax error should fail")

		// Connection should still be usable for valid queries
		var result string
		err = conn.QueryRow(s.t.Context(), `db.runCommand({ping: 1})`).Scan(&result)
		require.NoError(t, err, "Connection should still be usable after error")
		require.Contains(t, result, "ok")
	})
}

// ========================================
// Concurrent Connections Tests
// ========================================

func (s PgwireMongoSuite) Test_ConcurrentConnections() {
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			output, err := s.psql(fmt.Sprintf(`db.%s.find({_id: %d})`, s.testCollection(), (id%3)+1))
			if err != nil {
				errors <- fmt.Errorf("connection %d failed: %w (output: %s)", id, err, output)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		require.NoError(s.t, err)
	}
}

// ========================================
// Sequential Connections Tests
// ========================================

func (s PgwireMongoSuite) Test_MultipleSequentialConnections() {
	for i := range 10 {
		output, err := s.psql(fmt.Sprintf(`db.%s.find({_id: %d})`, s.testCollection(), (i%3)+1))
		require.NoError(s.t, err, "Sequential connection %d should succeed", i)
		require.NotEmpty(s.t, output)
	}
}

// ========================================
// Idle Timeout Tests
// ========================================

func (s PgwireMongoSuite) Test_IdleTimeout_Short() {
	dsn := pgwireDSN(s.peer.Name, map[string]string{"idle_timeout": "1"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	// First query should succeed
	var result string
	err = conn.QueryRow(s.t.Context(), `db.runCommand({ping: 1})`).Scan(&result)
	require.NoError(s.t, err, "First query should succeed")
	require.Contains(s.t, result, "ok")

	// Wait for idle timeout to expire
	time.Sleep(1500 * time.Millisecond)

	// Second query should fail due to closed connection
	err = conn.QueryRow(s.t.Context(), `db.runCommand({ping: 1})`).Scan(&result)
	require.Error(s.t, err, "Query should fail after idle timeout")
}
