package connmongo

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type iterationType int

const (
	// idle returns false on Next() with context.DeadlineExceeded
	idle iterationType = iota
	// insert returns true on Next() with a generated insert event
	insert
)

type mockChangeStream struct {
	err         error
	resumeToken bson.Raw
	current     bson.Raw

	idx          int
	iterations   []iterationType
	emittedTimes []time.Time

	t *testing.T
}

func newMockChangeStream(t *testing.T, iter ...iterationType) *mockChangeStream {
	t.Helper()
	return &mockChangeStream{t: t, iterations: iter}
}

func (cs *mockChangeStream) Next(context.Context) bool {
	if cs.idx >= len(cs.iterations) {
		cs.t.Fatalf("mockChangeStream: Next past end of mocked iterations (%d iterations)", len(cs.iterations))
	}
	ts := time.Now()
	cs.emittedTimes = append(cs.emittedTimes, ts)
	cs.resumeToken = toResumeToken(ts)

	label := cs.iterations[cs.idx]
	cs.idx++

	switch label {
	case insert:
		cs.current = newInsertChangeEvent(bson.NewObjectID(), ts)
		cs.err = nil
		return true
	case idle:
		cs.err = context.DeadlineExceeded
		return false
	default:
		cs.t.Fatalf("mockChangeStream: unknown label %d", label)
		return false
	}
}

func (cs *mockChangeStream) ResumeToken() bson.Raw { return cs.resumeToken }
func (cs *mockChangeStream) Err() error            { return cs.err }
func (cs *mockChangeStream) Current() bson.Raw     { return cs.current }
func (cs *mockChangeStream) Close() error          { return nil }

var _ ChangeStream = (*mockChangeStream)(nil)

type mockMetadataStore struct{ persisted []model.CdcCheckpoint }

func (ms *mockMetadataStore) GetLastOffset(context.Context, string) (model.CdcCheckpoint, error) {
	if n := len(ms.persisted); n > 0 {
		return ms.persisted[n-1], nil
	}
	return model.CdcCheckpoint{}, nil
}

func (ms *mockMetadataStore) SetLastOffset(_ context.Context, _ string, off model.CdcCheckpoint) error {
	ms.persisted = append(ms.persisted, off)
	return nil
}

func drainMongoCDCRecordsAsync(t *testing.T, stream *model.CDCStream[model.RecordItems]) {
	t.Helper()
	go func() {
		for range stream.GetRecords() {
		}
	}()
}

func newInsertChangeEvent(id bson.ObjectID, ts time.Time) bson.Raw {
	wallTime := ts.UTC().Truncate(time.Millisecond)
	event, _ := bson.Marshal(bson.D{
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: "db"},
			{Key: "coll", Value: "coll"},
		}},
		{Key: "operationType", Value: "insert"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "fullDocument", Value: bson.D{
			{Key: "_id", Value: id},
			{Key: "val", Value: "test"},
		}},
		{Key: "clusterTime", Value: toBsonTs(ts)},
		{Key: "wallTime", Value: wallTime},
	})
	return event
}

func TestChangeStreamIdleConnectionAdvancesOffset(t *testing.T) {
	ctx := t.Context()

	mockCS := newMockChangeStream(t, idle, idle, insert, idle)
	mockStore := &mockMetadataStore{}
	connector := &MongoConnector{
		logger: internal.LoggerFromCtx(t.Context()),
		createChangeStream: func(
			context.Context, mongo.Pipeline, ...options.Lister[options.ChangeStreamOptions],
		) (ChangeStream, error) {
			return mockCS, nil
		},
		metadataStore: mockStore,
	}

	otelManager, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)

	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:            "test_mongo_idle",
		RecordStream:           model.NewCDCStream[model.RecordItems](100),
		TableNameMapping:       map[string]model.NameAndExclude{"db.coll": {Name: "db_coll"}},
		TableNameSchemaMapping: map[string]*protos.TableSchema{},
		MaxBatchSize:           10000,
		IdleTimeout:            time.Minute,
	}
	drainMongoCDCRecordsAsync(t, req.RecordStream)

	require.NoError(t, connector.PullRecords(ctx, shared.CatalogPool{}, otelManager, req))
	require.Len(t, mockStore.persisted, 2)
	require.Equal(t, b64(toResumeToken(mockCS.emittedTimes[0])), mockStore.persisted[0].Text)
	require.Equal(t, b64(toResumeToken(mockCS.emittedTimes[1])), mockStore.persisted[1].Text)
	require.Equal(t, b64(toResumeToken(mockCS.emittedTimes[3])), req.RecordStream.GetLastCheckpoint().Text)
}

func b64(raw bson.Raw) string {
	return base64.StdEncoding.EncodeToString(raw)
}

func toBsonTs(ts time.Time) bson.Timestamp {
	return bson.Timestamp{T: uint32(ts.Unix()), I: uint32(ts.Nanosecond())}
}

func toResumeToken(ts time.Time) bson.Raw {
	t := toBsonTs(ts)
	keyString := make([]byte, 9)
	keyString[0] = byte(kTimestamp)
	binary.BigEndian.PutUint64(keyString[1:], uint64(t.T)<<32|uint64(t.I))
	raw, _ := bson.Marshal(bson.D{{Key: "_data", Value: hex.EncodeToString(keyString)}})
	return raw
}

func TestResumeTokenHelpersRoundTrip(t *testing.T) {
	ts := time.Now().UTC()
	rt := toResumeToken(ts)
	bsonTs, err := decodeTimestampFromResumeToken(rt)
	require.NoError(t, err)
	require.Equal(t, toBsonTs(ts), bsonTs)
}

func TestCreatePipeline(t *testing.T) {
	tableNameMapping := map[string]model.NameAndExclude{"db.coll": {Name: "db_coll"}}

	// lookupInPipeline returns the first value found at the given path in any pipeline stage.
	lookupInPipeline := func(t *testing.T, pipeline mongo.Pipeline, path ...string) bson.RawValue {
		t.Helper()
		for _, stage := range pipeline {
			raw, err := bson.Marshal(stage)
			require.NoError(t, err)
			if value, err := bson.Raw(raw).LookupErr(path...); err == nil {
				return value
			}
		}
		return bson.RawValue{}
	}

	excludedOpsInPipeline := func(t *testing.T, pipeline mongo.Pipeline) []string {
		t.Helper()
		ninValue := lookupInPipeline(t, pipeline, "$match", "operationType", "$nin")
		if ninValue.IsZero() {
			return nil
		}
		values, err := ninValue.Array().Values()
		require.NoError(t, err)
		ops := make([]string, 0, len(values))
		for _, value := range values {
			ops = append(ops, value.StringValue())
		}
		return ops
	}

	requireProjectFields := func(t *testing.T, pipeline mongo.Pipeline) {
		t.Helper()
		for _, field := range []string{"operationType", "clusterTime", "documentKey", "fullDocument", "ns"} {
			require.False(t, lookupInPipeline(t, pipeline, "$project", field).IsZero())
		}
	}

	t.Run("pipeline without filters", func(t *testing.T) {
		pipeline, err := createPipeline(nil, nil)
		require.NoError(t, err)

		requireProjectFields(t, pipeline)
		require.True(t, lookupInPipeline(t, pipeline, "$match").IsZero())
	})

	t.Run("pipeline with table mapping", func(t *testing.T) {
		pipeline, err := createPipeline(tableNameMapping, nil)
		require.NoError(t, err)

		requireProjectFields(t, pipeline)
		require.Equal(t, "db", lookupInPipeline(t, pipeline, "$match", "$or", "0", "$and", "0", "ns.db").StringValue())
		require.Equal(t, "coll", lookupInPipeline(t, pipeline, "$match", "$or", "0", "$and", "1", "ns.coll", "$in", "0").StringValue())
		require.Empty(t, excludedOpsInPipeline(t, pipeline))
	})

	t.Run("pipeline with excluded operation types", func(t *testing.T) {
		pipeline, err := createPipeline(nil, []operationType{operationTypeDelete})
		require.NoError(t, err)

		requireProjectFields(t, pipeline)
		require.Equal(t, []string{"delete"}, excludedOpsInPipeline(t, pipeline))
		require.Empty(t, lookupInPipeline(t, pipeline, "$match", "$or", "0", "$and", "1", "ns.coll", "$in", "0"))
		require.Empty(t, lookupInPipeline(t, pipeline, "$match", "$or", "0", "$and", "0", "ns.db"))
	})
}

func TestExcludedOperationTypes(t *testing.T) {
	envKey := "PEERDB_MONGODB_EXCLUDED_OPERATION_TYPES"
	resolve := func(t *testing.T, env map[string]string) ([]operationType, error) {
		t.Helper()
		connector := &MongoConnector{logger: internal.LoggerFromCtx(t.Context())}
		if err := connector.SetupReplConn(t.Context(), env); err != nil {
			return nil, err
		}
		return connector.excludedOps, nil
	}

	t.Run("valid operation types", func(t *testing.T) {
		ops, err := resolve(t, map[string]string{envKey: "Delete, UPDATE"})
		require.NoError(t, err)
		require.Equal(t, []operationType{operationTypeDelete, operationTypeUpdate}, ops)
	})

	t.Run("empty", func(t *testing.T) {
		ops, err := resolve(t, map[string]string{envKey: ""})
		require.NoError(t, err)
		require.Empty(t, ops)
	})

	t.Run("invalid operation type is ignored", func(t *testing.T) {
		ops, err := resolve(t, map[string]string{envKey: "delete,drop"})
		require.NoError(t, err)
		require.Equal(t, []operationType{operationTypeDelete}, ops)
	})

	t.Run("bad input is ignored", func(t *testing.T) {
		ops, err := resolve(t, map[string]string{envKey: "123, bad input"})
		require.NoError(t, err)
		require.Empty(t, ops)
	})
}

func TestDecodeEvent(t *testing.T) {
	id := bson.NewObjectID()
	insertTs := time.Now().UTC()
	insertWallTime := insertTs.Truncate(time.Millisecond)
	deleteTs := time.Now().Add(time.Second).UTC()

	mustMarshal := func(v any) bson.Raw {
		t.Helper()
		raw, err := bson.Marshal(v)
		require.NoError(t, err)
		return raw
	}

	deleteRaw := mustMarshal(bson.D{
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: "db"},
			{Key: "coll", Value: "coll"},
		}},
		{Key: "operationType", Value: "delete"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "clusterTime", Value: toBsonTs(deleteTs)},
	})

	deleteWithNullFullDocRaw := mustMarshal(bson.D{
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: "db"},
			{Key: "coll", Value: "coll"},
		}},
		{Key: "operationType", Value: "delete"},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "fullDocument", Value: nil},
		{Key: "clusterTime", Value: toBsonTs(deleteTs)},
	})

	fullDocument := mustMarshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "val", Value: "test"},
	})

	cases := []struct {
		name    string
		wantErr string
		raw     bson.Raw
		want    ChangeEvent
	}{
		{
			name: "insert populates every field",
			raw:  newInsertChangeEvent(id, insertTs),
			want: ChangeEvent{
				Ns:            Namespace{Db: "db", Coll: "coll"},
				OperationType: "insert",
				DocumentKey:   mustMarshal(bson.D{{Key: "_id", Value: id}}),
				FullDocument:  &fullDocument,
				ClusterTime:   toBsonTs(insertTs),
				WallTime:      &insertWallTime,
			},
		},
		{
			name: "delete omits fullDocument",
			raw:  deleteRaw,
			want: ChangeEvent{
				Ns:            Namespace{Db: "db", Coll: "coll"},
				OperationType: "delete",
				DocumentKey:   mustMarshal(bson.D{{Key: "_id", Value: id}}),
				FullDocument:  nil,
				ClusterTime:   toBsonTs(deleteTs),
			},
		},
		{ // Behaviour before go.mongodb.org/mongo-driver/v2 v2.6.0
			name: "delete with null fullDocument decodes as a delete with nil fullDocument",
			raw:  deleteWithNullFullDocRaw,
			want: ChangeEvent{
				Ns:            Namespace{Db: "db", Coll: "coll"},
				OperationType: "delete",
				DocumentKey:   mustMarshal(bson.D{{Key: "_id", Value: id}}),
				FullDocument:  nil,
				ClusterTime:   toBsonTs(deleteTs),
			},
		},
		{
			name: "empty document decodes to zero value",
			raw:  mustMarshal(bson.D{}),
			want: ChangeEvent{},
		},
		{
			name:    "invalid bson returns wrapped error",
			raw:     bson.Raw{0x05, 0x00, 0x00, 0x00, 0xff},
			wantErr: "failed to decode change stream document",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got ChangeEvent
			err := decodeEvent(tc.raw, &got)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCreatePipelineProjectsWallTime(t *testing.T) {
	pipeline, err := createPipeline(nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, pipeline)

	projectStage := pipeline[len(pipeline)-1]
	require.Len(t, projectStage, 1)
	projectFields, ok := projectStage[0].Value.(bson.D)
	require.True(t, ok)
	require.Contains(t, projectFields, bson.E{Key: "wallTime", Value: 1})
}
