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

//nolint:govet // it's a test, no need for fieldalignment
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

func (cs *mockChangeStream) ResumeToken() bson.Raw       { return cs.resumeToken }
func (cs *mockChangeStream) Err() error                  { return cs.err }
func (cs *mockChangeStream) Current() bson.Raw           { return cs.current }
func (cs *mockChangeStream) Close(context.Context) error { return nil }

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

func TestDecodeEvent(t *testing.T) {
	id := bson.NewObjectID()
	insertTs := time.Now().UTC()
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

	cases := []struct {
		name    string
		raw     bson.Raw
		want    ChangeEvent
		wantErr string
	}{
		{
			name: "insert populates every field",
			raw:  newInsertChangeEvent(id, insertTs),
			want: ChangeEvent{
				Ns:            Namespace{Db: "db", Coll: "coll"},
				OperationType: "insert",
				DocumentKey:   mustMarshal(bson.D{{Key: "_id", Value: id}}),
				FullDocument: mustMarshal(bson.D{
					{Key: "_id", Value: id},
					{Key: "val", Value: "test"},
				}),
				ClusterTime: toBsonTs(insertTs),
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
			name: "delete with null fullDocument decodes as a delete with an empty fullDocument",
			raw:  deleteWithNullFullDocRaw,
			want: ChangeEvent{
				Ns:            Namespace{Db: "db", Coll: "coll"},
				OperationType: "delete",
				DocumentKey:   mustMarshal(bson.D{{Key: "_id", Value: id}}),
				FullDocument:  bson.Raw{},
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
