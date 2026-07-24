package e2e

import (
	"context"
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
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type SwitchboardMongoSuite struct {
	t      *testing.T
	source *MongoSource
	peer   *protos.Peer
	suffix string
}

func (s SwitchboardMongoSuite) T() *testing.T {
	return s.t
}

func (s SwitchboardMongoSuite) Teardown(ctx context.Context) {
	s.source.Teardown(s.t, ctx, s.suffix)
}

func SetupSwitchboardMongoSuite(t *testing.T) SwitchboardMongoSuite {
	t.Helper()

	suffix := "pgwmo_" + strings.ToLower(common.RandomString(8))
	source, err := SetupMongo(t, suffix)
	if err != nil {
		t.Skipf("MongoDB setup failed: %v", err)
	}

	testDb := GetTestDatabase(suffix)

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

	conn, err := pgx.Connect(t.Context(), switchboardDSN(peer.Name, nil))
	if err != nil {
		source.Teardown(t, t.Context(), suffix)
		t.Fatalf("Switchboard not available: %v", err)
	}
	conn.Close(t.Context())

	return SwitchboardMongoSuite{
		t:      t,
		source: source,
		peer:   peer,
		suffix: suffix,
	}
}

func TestSwitchboardMongo(t *testing.T) {
	e2eshared.RunSuite(t, SetupSwitchboardMongoSuite)
}

func (s SwitchboardMongoSuite) psql(query string) (string, error) {
	return runPsql(s.t, s.peer.Name, "-tA", "-c", query)
}

func (s SwitchboardMongoSuite) testCollection() string {
	return "test_collection"
}

func (s SwitchboardMongoSuite) testDatabase() string {
	return GetTestDatabase(s.suffix)
}

func (s SwitchboardMongoSuite) queryWithOptions(query string, options map[string]string) error {
	dsn := switchboardDSN(s.peer.Name, options)
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
// Result Paths (scalar, cursor, adminDB, formatter)
// ========================================

func (s SwitchboardMongoSuite) Test_Scalar() {
	output, err := s.psql(`{"ping": 1}`)
	require.NoError(s.t, err)
	require.Contains(s.t, output, `"ok"`)
}

func (s SwitchboardMongoSuite) Test_Cursor() {
	output, err := s.psql(fmt.Sprintf(`{"find": "%s", "filter": {}}`, s.testCollection()))
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "bob")
	require.Contains(s.t, output, "charlie")
}

func (s SwitchboardMongoSuite) Test_CursorAggregate() {
	query := fmt.Sprintf(
		`{"aggregate": "%s", "pipeline": [{"$match": {"active": true}}], "cursor": {}}`,
		s.testCollection(),
	)
	output, err := s.psql(query)
	require.NoError(s.t, err)
	require.Contains(s.t, output, "alice")
	require.Contains(s.t, output, "charlie")
	require.NotContains(s.t, output, "bob")
}

func (s SwitchboardMongoSuite) Test_AdminDB() {
	output, err := s.psql(`{"listDatabases": 1, "nameOnly": true}`)
	require.NoError(s.t, err)
	require.NotEmpty(s.t, output)
}

// NOTE: psql behavior differs in interactive vs non-interactive contexts
// Interactive mode intercepts commands like "help", and there's no practical way to test that.
func (s SwitchboardMongoSuite) Test_Help() {
	output, err := s.psql(`show help`)
	require.NoError(s.t, err)

	// Structural sections
	require.Contains(s.t, output, "Input Format:")
	require.Contains(s.t, output, "Shell Commands:")
	require.Contains(s.t, output, "use <dbname>")
	require.Contains(s.t, output, "Allowed Wire Commands:")

	// Every category from the allowlist must appear
	for _, section := range []string{"Query", "User/Role Info", "Replication", "Sharding", "Sessions", "Admin", "Diagnostic"} {
		require.Contains(s.t, output, section+":", "missing section %q", section)
	}

	// Spot-check representative commands from different categories
	for _, cmd := range []string{"find", "aggregate", "ping", "listCollections", "listDatabases", "explain", "serverStatus"} {
		require.Contains(s.t, output, cmd, "missing command %q", cmd)
	}

	// Each command should have a docs link
	require.Contains(s.t, output, "https://www.mongodb.com/docs/manual/reference/command/")
}

// ========================================
// Database Switching
// ========================================

func (s SwitchboardMongoSuite) Test_UseDatabase() {
	otherDb := s.testDatabase() + "_other"
	otherCollection := s.source.AdminClient().Database(otherDb).Collection("other_collection")
	s.t.Cleanup(func() {
		_ = s.source.AdminClient().Database(otherDb).Drop(s.t.Context())
	})

	_, err := otherCollection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: 1},
		{Key: "name", Value: "from_other_db"},
	})
	require.NoError(s.t, err)

	dsn := switchboardDSN(s.peer.Name, nil)
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	var result string
	err = conn.QueryRow(s.t.Context(), "use "+otherDb).Scan(&result)
	require.NoError(s.t, err)
	require.Contains(s.t, result, "switched to db")

	err = conn.QueryRow(s.t.Context(), `{"find": "other_collection", "filter": {}}`).Scan(&result)
	require.NoError(s.t, err)
	require.Contains(s.t, result, "from_other_db")

	err = conn.QueryRow(s.t.Context(), "use "+s.testDatabase()).Scan(&result)
	require.NoError(s.t, err)

	err = conn.QueryRow(s.t.Context(),
		fmt.Sprintf(`{"find": "%s", "filter": {}}`, s.testCollection()),
	).Scan(&result)
	require.NoError(s.t, err)
	require.Contains(s.t, result, "alice")
}

// ========================================
// Denied Operations
// ========================================

func (s SwitchboardMongoSuite) Test_DeniedOperations() {
	tests := []struct {
		name  string
		query string
	}{
		{"Insert", `{"insert": "test", "documents": [{"a": 1}]}`},
		{"Update", `{"update": "test", "updates": [{"q": {}, "u": {"$set": {"a": 1}}}]}`},
		{"Delete", `{"delete": "test", "deletes": [{"q": {}, "limit": 1}]}`},
		{"Drop", `{"drop": "test"}`},
		{"DropDatabase", `{"dropDatabase": 1}`},
	}

	for _, tt := range tests {
		s.t.Run(tt.name, func(t *testing.T) {
			_, err := s.psql(tt.query)
			require.Error(t, err, "Operation %s should be denied", tt.name)
		})
	}
}

// ========================================
// Guardrails
// ========================================

func (s SwitchboardMongoSuite) Test_Guardrails_MaxRows() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("guardrails_test")
	docs := make([]any, 20)
	for i := range 20 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "value", Value: i}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	err = s.queryWithOptions(`{"find": "guardrails_test", "filter": {}}`, map[string]string{"max_rows": "5"})
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "row limit")
}

func (s SwitchboardMongoSuite) Test_Guardrails_MaxBytes() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("bytes_test")
	largeString := strings.Repeat("x", 1000)
	_, err := collection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: "large"},
		{Key: "data", Value: largeString},
	})
	require.NoError(s.t, err)

	err = s.queryWithOptions(`{"find": "bytes_test", "filter": {}}`, map[string]string{"max_bytes": "100"})
	require.Error(s.t, err)
	require.Contains(s.t, err.Error(), "byte limit")
}

func (s SwitchboardMongoSuite) Test_Guardrails_ResumeAfterRowLimit() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("resume_test")
	docs := make([]any, 10)
	for i := range 10 {
		docs[i] = bson.D{{Key: "_id", Value: i}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_rows": "3"})
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	rows, err := conn.Query(s.t.Context(), `{"find": "resume_test", "filter": {}}`)
	require.NoError(s.t, err, "Query should start successfully")

	for rows.Next() {
	}
	rowErr := rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to row limit")
	require.Contains(s.t, rowErr.Error(), "row limit")

	var result string
	err = conn.QueryRow(s.t.Context(), `{"ping": 1}`).Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after limit exceeded")
	require.Contains(s.t, result, "ok")
}

func (s SwitchboardMongoSuite) Test_Guardrails_ResumeAfterByteLimit() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("resume_bytes_test")
	largeString := strings.Repeat("y", 500)
	_, err := collection.InsertOne(s.t.Context(), bson.D{
		{Key: "_id", Value: "large"},
		{Key: "data", Value: largeString},
	})
	require.NoError(s.t, err)

	dsn := switchboardDSN(s.peer.Name, map[string]string{"max_bytes": "500"})
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	rows, err := conn.Query(s.t.Context(), `{"find": "resume_bytes_test", "filter": {}}`)
	require.NoError(s.t, err)

	for rows.Next() {
	}
	rowErr := rows.Err()
	rows.Close()

	require.Error(s.t, rowErr, "Query should fail due to byte limit")
	require.Contains(s.t, rowErr.Error(), "byte limit")

	var result string
	err = conn.QueryRow(s.t.Context(), `{"ping": 1}`).Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after byte limit exceeded")
	require.Contains(s.t, result, "ok")
}

// ========================================
// Cancel Request
// ========================================

func (s SwitchboardMongoSuite) Test_CancelRequest() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("cancel_test")
	docs := make([]any, 10000)
	for i := range 10000 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "data", Value: strings.Repeat("x", 100)}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	dsn := switchboardDSN(s.peer.Name, nil)
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	errCh := make(chan error, 1)
	go func() {
		rows, err := conn.Query(s.t.Context(),
			`{"aggregate": "cancel_test", "pipeline": [{"$sort": {"data": 1}}, {"$group": {"_id": "$data"}}], "cursor": {}}`,
		)
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
	require.NoError(s.t, err, "Cancel request should be sent")

	select {
	case queryErr := <-errCh:
		if queryErr != nil {
			s.t.Logf("Query error (expected if canceled): %v", queryErr)
		}
	case <-time.After(10 * time.Second):
		s.t.Fatal("Query should have completed or been canceled within 10 seconds")
	}
}

// ========================================
// Error Handling
// ========================================

func (s SwitchboardMongoSuite) Test_Error_InvalidJSON() {
	_, err := s.psql(`{invalid json`)
	require.Error(s.t, err)
}

func (s SwitchboardMongoSuite) Test_Error_PsqlBackslashD() {
	output, err := s.psql(`\d`)
	require.Error(s.t, err)
	require.Contains(s.t, output, "PostgreSQL catalog queries not supported")
	require.Contains(s.t, output, "show collections")
}

func (s SwitchboardMongoSuite) Test_Error_InvalidCommand() {
	_, err := s.psql(`{"invalidCommand": 1}`)
	require.Error(s.t, err)
}

func (s SwitchboardMongoSuite) Test_Error_ConnectionRecovery() {
	dsn := switchboardDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), `{invalid`)
	require.Error(s.t, err)

	var result string
	err = conn.QueryRow(s.t.Context(), `{"ping": 1}`).Scan(&result)
	require.NoError(s.t, err, "Connection should still be usable after error")
	require.Contains(s.t, result, "ok")
}

// ========================================
// Large Results
// ========================================

func (s SwitchboardMongoSuite) Test_LargeResults() {
	collection := s.source.AdminClient().Database(s.testDatabase()).Collection("large_test")
	docs := make([]any, 1000)
	for i := range 1000 {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "name", Value: fmt.Sprintf("doc_%d", i)}}
	}
	_, err := collection.InsertMany(s.t.Context(), docs)
	require.NoError(s.t, err)

	output, err := s.psql(`{"find": "large_test", "filter": {}}`)
	require.NoError(s.t, err)

	count := strings.Count(output, `"_id"`)
	require.Equal(s.t, 1000, count, "Should return all 1000 documents")
}

// ========================================
// Empty Query
// ========================================

func (s SwitchboardMongoSuite) Test_EmptyQuery() {
	dsn := switchboardDSN(s.peer.Name, nil)
	cfg, _ := pgx.ParseConfig(dsn)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	_, err = conn.Exec(s.t.Context(), ";")
	require.NoError(s.t, err, "Empty query should succeed")
}

// ========================================
// Connection Lifecycle
// ========================================

func (s SwitchboardMongoSuite) Test_ConcurrentConnections() {
	const numConns = 5
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			output, err := s.psql(fmt.Sprintf(
				`{"find": "%s", "filter": {"_id": %d}}`, s.testCollection(), (id%3)+1,
			))
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

func (s SwitchboardMongoSuite) Test_SequentialConnections() {
	for i := range 10 {
		output, err := s.psql(fmt.Sprintf(
			`{"find": "%s", "filter": {"_id": %d}}`, s.testCollection(), (i%3)+1,
		))
		require.NoError(s.t, err, "Sequential connection %d should succeed", i)
		require.NotEmpty(s.t, output)
	}
}

func (s SwitchboardMongoSuite) Test_IdleTimeout() {
	dsn := switchboardDSN(s.peer.Name, map[string]string{"idle_timeout": "1"})

	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(s.t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(s.t.Context(), cfg)
	require.NoError(s.t, err)
	defer conn.Close(s.t.Context())

	var result string
	err = conn.QueryRow(s.t.Context(), `{"ping": 1}`).Scan(&result)
	require.NoError(s.t, err)
	require.Contains(s.t, result, "ok")

	time.Sleep(1500 * time.Millisecond)

	err = conn.QueryRow(s.t.Context(), `{"ping": 1}`).Scan(&result)
	require.Error(s.t, err, "Query should fail after idle timeout")
}
