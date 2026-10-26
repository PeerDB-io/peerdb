package connpubsub

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// failingPublish fails every Publish RPC with a code the client does not retry.
// The first one is held for a while first, so the consumer waits on it while
// the feeder fills the pipeline behind it.
type failingPublish struct {
	hold time.Duration
	held atomic.Bool
}

func (r *failingPublish) React(_ any) (bool, any, error) {
	if r.held.CompareAndSwap(false, true) {
		time.Sleep(r.hold)
	}
	return true, &pubsubpb.PublishResponse{}, status.Error(codes.PermissionDenied, "injected publish failure")
}

// A failed publish RPC used to deadlock SyncRecords. Under load the pipeline is
// full: the consumer waits on a batch, the pool is blocked sending its next
// result, and the main loop is blocked in pool.Run. When the batch then fails,
// the consumer stopped reading after one result, which freed one slot: the pool
// sent one more message result and blocked on that record's LSN sentinel for
// good, and the main loop never got back to the select that would see the
// cancelled batch. The activity kept heartbeating, so the mirror sat in
// STATUS_RUNNING until someone paused it.
//
// Runs against an in-process Pub/Sub fake whose first publish RPC is held back
// long enough for the pipeline to fill and then fails, as a real outage would.
func TestSyncRecordsFailsInsteadOfHangingOnPublishError(t *testing.T) {
	ctx := t.Context()

	srv := pstest.NewServer(pstest.ServerReactorOption{
		FuncName: "Publish",
		Reactor:  &failingPublish{hold: 2 * time.Second},
	})
	defer srv.Close()
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client, err := pubsub.NewClient(ctx, "peerdb-test", option.WithGRPCConn(conn))
	require.NoError(t, err)
	defer client.Close()

	topic := "deadlock_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/peerdb-test/topics/" + topic})
	require.NoError(t, err)

	// No catalog behind this connector: every setting is pinned through Env and
	// the flush loop never fires. The record stream is never closed, so the only
	// way out of SyncRecords is the failure path, which never reaches the
	// catalog-backed FinishBatch.
	c := &PubSubConnector{client: client, logger: internal.LoggerFromCtx(ctx)}
	const buffered = 1024
	stream := model.NewCDCStream[model.RecordItems](buffered)
	var consumed atomic.Int64
	req := &model.SyncRecordsRequest[model.RecordItems]{
		Records:        stream,
		ConsumedOffset: &consumed,
		FlowJobName:    "pubsub_deadlock_test",
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      "public." + topic,
			DestinationTableIdentifier: topic,
		}},
		Env: map[string]string{
			"PEERDB_QUEUE_PARALLELISM":           "1",
			"PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS": "3600",
			"PEERDB_QUEUE_FORCE_TOPIC_CREATION":  "false",
		},
		SyncBatchID: 1,
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.SyncRecords(ctx, req)
		done <- err
	}()

	feedCtx, stopFeeding := context.WithCancel(ctx)
	defer stopFeeding()
	var fed atomic.Int64
	go func() {
		for i := int64(1); feedCtx.Err() == nil; i++ {
			items := model.NewRecordItems(1)
			items.AddColumn("payload", types.QValueString{Val: "x"})
			record := &model.InsertRecord[model.RecordItems]{
				Items:                items,
				SourceTableName:      "public." + topic,
				DestinationTableName: topic,
				BaseRecord: model.BaseRecord{
					CheckpointID:   i,
					CommitTimeNano: time.Now().UnixNano(),
				},
			}
			if err := stream.AddRecord(feedCtx, record); err != nil {
				return
			}
			fed.Store(i)
		}
	}()

	// The stream backs up once the main loop is blocked behind the full pipeline,
	// which is the state the hang needs; with parallelism 1 the loop has taken a
	// handful of records only after the pool merged the first one.
	require.Eventually(t, func() bool {
		return stream.ChannelLen() == buffered && fed.Load()-buffered >= 8
	}, 10*time.Second, 10*time.Millisecond)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "error publishing message")
	case <-time.After(30 * time.Second):
		t.Fatal("SyncRecords did not return after a failed publish")
	}
}
