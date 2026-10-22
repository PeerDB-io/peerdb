//go:build tilt

package connmongo

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	peerdbmongo "github.com/PeerDB-io/peerdb/flow/pkg/mongo"
	"github.com/PeerDB-io/peerdb/flow/pkg/testutil"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TestPullRecordsReusesRealCursorAcrossAcknowledgedBatches(t *testing.T) {
	testutil.LoadEnv()
	creds := testutil.MongoAdminTestCredentials(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	clientOptions, err := peerdbmongo.BuildClientOptions(peerdbmongo.ClientConfig{
		Uri:            creds.URI,
		Username:       creds.Username,
		Password:       creds.Password,
		ReadPreference: peerdbmongo.ReadPreferencePrimary,
		DisableTls:     true,
	})
	require.NoError(t, err)
	clientOptions.SetWriteConcern(writeconcern.Majority())
	var aggregates, getMores, serverBatchSize atomic.Int32
	clientOptions.SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			switch evt.CommandName {
			case "aggregate":
				aggregates.Add(1)
			case "getMore":
				getMores.Add(1)
			}
		},
		Succeeded: func(_ context.Context, evt *event.CommandSucceededEvent) {
			if evt.CommandName == "getMore" {
				if batch, ok := evt.Reply.Lookup("cursor", "nextBatch").ArrayOK(); ok {
					if values, err := batch.Values(); err == nil {
						serverBatchSize.Store(int32(len(values)))
					}
				}
			}
		},
	})
	client, err := mongo.Connect(clientOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, client.Disconnect(cleanupCtx))
	})

	db := client.Database("cursor_reuse_" + strings.ToLower(common.RandomString(8)))
	require.NoError(t, db.CreateCollection(ctx, "records"))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, db.Drop(cleanupCtx))
	})
	collection := db.Collection("records")
	connector := &MongoConnector{
		client:           client,
		logger:           internal.LoggerFromCtx(ctx),
		metadataStore:    &mockMetadataStore{},
		numDecodeWorkers: 1,
	}
	t.Cleanup(func() { require.NoError(t, connector.closeRetainedChangeStream()) })
	var driverStream *mongo.ChangeStream
	watchCalls := 0
	connector.createChangeStream = func(
		ctx context.Context, pipeline mongo.Pipeline, opts ...options.Lister[options.ChangeStreamOptions],
	) (ChangeStream, error) {
		watchCalls++
		stream, err := client.Watch(ctx, pipeline, opts...)
		if err != nil {
			return nil, err
		}
		if watchCalls == 1 {
			driverStream = stream
			// Write after Watch establishes its position but before the first getMore,
			// so the server can return all four events in one batch.
			_, err = collection.InsertMany(ctx, []any{
				bson.D{{Key: "_id", Value: "first"}},
				bson.D{{Key: "_id", Value: "second"}},
				bson.D{{Key: "_id", Value: "third"}},
				bson.D{{Key: "_id", Value: "fourth"}},
			})
			if err != nil {
				_ = stream.Close(ctx)
				return nil, err
			}
		}
		return &changeStreamWrapper{ChangeStream: stream}, nil
	}
	metrics, err := otel_metrics.NewOtelManager(ctx, "test", false)
	require.NoError(t, err)
	req := &model.PullRecordsRequest[model.RecordItems]{
		FlowJobName:      t.Name(),
		TableNameMapping: map[string]model.SourceTableMapping{db.Name() + ".records": {Name: "records"}},
		MaxBatchSize:     2,
		IdleTimeout:      time.Minute,
		InternalVersion:  shared.InternalVersion_Latest,
	}
	var cursorID int64
	wantGetMores, wantServerBatchSize := int32(1), int32(4)
	for batch, wantIDs := range [][]string{{"first", "second"}, {"third", "fourth"}, {"fifth", "sixth"}} {
		if batch == 2 {
			// The buffered events are drained. A fresh server read must also work
			// after the earlier pulls' contexts were canceled.
			_, err := collection.InsertMany(ctx, []any{
				bson.D{{Key: "_id", Value: "fifth"}},
				bson.D{{Key: "_id", Value: "sixth"}},
			})
			require.NoError(t, err)
			wantGetMores, wantServerBatchSize = 2, 2
		}
		req.RecordStream = model.NewCDCStream[model.RecordItems](2)
		batchCtx, batchCancel := context.WithCancel(ctx)
		pullErr := connector.PullRecords(batchCtx, shared.CatalogPool{}, metrics, req)
		// SyncFlow's errgroup cancels its batch context before the next pull.
		batchCancel()
		require.NoError(t, pullErr)
		var ids []string
		for record := range req.RecordStream.GetRecords() {
			value, err := record.GetItems().GetValueByColName(DefaultDocumentKeyColumnName)
			require.NoError(t, err)
			ids = append(ids, value.Value().(string))
		}
		require.Equal(t, wantIDs, ids)
		require.NotNil(t, connector.changeStream)
		if batch == 0 {
			cursorID = driverStream.ID()
			require.NotZero(t, cursorID)
			require.Equal(t, 2, driverStream.RemainingBatchLength(), "keep unread server-batch events")
		} else {
			require.Zero(t, driverStream.RemainingBatchLength())
		}
		require.Equal(t, cursorID, connector.changeStream.stream.ID())
		require.Equal(t, 1, watchCalls)
		require.Equal(t, int32(1), aggregates.Load())
		require.Equal(t, wantGetMores, getMores.Load(), "only the first and third pulls need a server read")
		require.Equal(t, wantServerBatchSize, serverBatchSize.Load())
		checkpoint := req.RecordStream.GetLastCheckpoint()
		require.NotEmpty(t, checkpoint.Text)
		require.NotEqual(t, req.LastOffset.Text, checkpoint.Text)
		require.NoError(t, connector.UpdateReplStateLastOffset(ctx, checkpoint))
		req.LastOffset = checkpoint
	}
}
