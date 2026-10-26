package connpubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	lua "github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pua"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PubSubConnector struct {
	*metadataStore.PostgresMetadata
	client *pubsub.Client
	logger log.Logger
}

func NewPubSubConnector(
	ctx context.Context,
	env map[string]string,
	config *protos.PubSubConfig,
) (*PubSubConnector, error) {
	sa := utils.GcpServiceAccountFromProto(config.ServiceAccount)
	client, err := sa.CreatePubSubClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &PubSubConnector{
		client:           client,
		PostgresMetadata: pgMetadata,
		logger:           internal.LoggerFromCtx(ctx),
	}, nil
}

func (c *PubSubConnector) Close() error {
	if c != nil {
		c.client.Close()
	}
	return nil
}

func (c *PubSubConnector) ConnectionActive(ctx context.Context) error {
	topicName := fmt.Sprintf("projects/%s/topics/%s", c.client.Project(), "test")
	if _, err := c.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: topicName,
	}); err != nil || status.Code(err) == codes.NotFound {
		return fmt.Errorf("pubsub connection active check failure: %w", err)
	}
	return nil
}

func (c *PubSubConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

func (c *PubSubConnector) ReplayTableSchemaDeltas(_ context.Context, _ map[string]string,
	flowJobName string, _ []*protos.TableMapping, schemaDeltas []*protos.TableSchemaDelta, _ []string,
) error {
	return nil
}

type PubSubMessage struct {
	*pubsub.Message
	Topic string
}

type poolResult struct {
	messages []PubSubMessage
	lsn      int64
}

type publishResult struct {
	*pubsub.PublishResult
	lsn int64
}

func lvalueToPubSubMessage(ls *lua.LState, value lua.LValue) (PubSubMessage, error) {
	var topic string
	var msg *pubsub.Message
	switch v := value.(type) {
	case lua.LString:
		msg = &pubsub.Message{
			Data: shared.UnsafeFastStringToReadOnlyBytes(string(v)),
		}
	case *lua.LTable:
		key, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "key"))
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("invalid key, %w", err)
		}
		value, err := utils.LVAsReadOnlyBytes(ls, ls.GetField(v, "value"))
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("invalid value, %w", err)
		}
		topic, err = utils.LVAsStringOrNil(ls, ls.GetField(v, "topic"))
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("invalid topic, %w", err)
		}
		msg = &pubsub.Message{
			OrderingKey: key,
			Data:        value,
		}
		lheaders := ls.GetField(v, "headers")
		if headers, ok := lheaders.(*lua.LTable); ok {
			msg.Attributes = make(map[string]string)
			headers.ForEach(func(k, v lua.LValue) {
				msg.Attributes[k.String()] = v.String()
			})
		} else if lua.LVAsBool(lheaders) {
			return PubSubMessage{}, fmt.Errorf("invalid headers, must be nil or table: %s", lheaders)
		}
	case *lua.LNilType:
	default:
		return PubSubMessage{}, fmt.Errorf("script returned invalid value: %s", value)
	}
	return PubSubMessage{
		Message: msg,
		Topic:   topic,
	}, nil
}

func (c *PubSubConnector) createPool(
	ctx context.Context,
	env map[string]string,
	script string,
	flowJobName string,
	topiccache *topicCache,
	publish chan<- publishResult,
	queueErr func(error),
) (*utils.LPool[poolResult], error) {
	maxSize, err := internal.PeerDBQueueParallelism(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get parallelism: %w", err)
	}

	return utils.LuaPool(int(maxSize), func() (*lua.LState, error) {
		ls, err := utils.LoadScript(ctx, script, utils.LuaPrintFn(func(s string) {
			_ = c.LogFlowInfo(ctx, flowJobName, s)
		}))
		if err != nil {
			return nil, fmt.Errorf("[pubsub] error loading script: %w", err)
		}
		if script == "" {
			ls.Env.RawSetString("onRecord", ls.NewFunction(utils.DefaultOnRecord))
		}
		return ls, nil
	}, func(result poolResult) {
		for _, message := range result.messages {
			// Nothing is published once the batch is cancelled: Publish ignores
			// the context, so anything queued now would only have to be flushed
			// or failed by Stop, and could land after the batch is reported failed.
			if ctx.Err() != nil {
				return
			}
			topic, err := topiccache.GetOrSet(message.Topic, func() (*pubsubpb.Topic, error) {
				topicName := fmt.Sprintf("projects/%s/topics/%s", c.client.Project(), message.Topic)
				topic, err := c.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
					Topic: topicName,
				})
				if err == nil {
					return topic, nil
				}
				// Every other outcome used to hand the cache a nil topic with no
				// error, which it then dereferenced.
				if status.Code(err) != codes.NotFound {
					return nil, err
				}
				force, envErr := internal.PeerDBQueueForceTopicCreation(ctx, env)
				if envErr != nil {
					return nil, envErr
				}
				if !force {
					return nil, err
				}
				newTopic, err := c.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
					Name: topicName,
				})
				if err != nil {
					return nil, fmt.Errorf("error creating topic: %w", err)
				}
				return newTopic, nil
			})
			if err != nil {
				queueErr(fmt.Errorf("[pubsub] error getting topic: %w", err))
				return
			}

			if message.OrderingKey != "" {
				topic.Publisher.EnableMessageOrdering = true
			}
			// Give up once the batch is cancelled, so a failed batch tears down
			// without depending on the consumer: a send that waited here would
			// block the pool and, through it, the SyncRecords loop in pool.Run.
			select {
			case publish <- publishResult{
				PublishResult: topic.Publisher.Publish(ctx, message.Message),
			}:
			case <-ctx.Done():
				return
			}
		}
		select {
		case publish <- publishResult{
			lsn: result.lsn,
		}:
		case <-ctx.Done():
		}
	})
}

// consumeResults confirms each publish in order and, when lastSeenLSN is given,
// records the LSN of every record whose messages all succeeded. Once the batch
// is cancelled, for a failed publish or for any error the pool reports, it
// keeps draining so no sender can ever block on the channel, but confirms
// nothing more: a sentinel that follows a failed record must not advance the
// offset the flush loop persists. Closes done once the channel is closed and
// drained.
func consumeResults(
	queueCtx context.Context,
	queueErr func(error),
	publish <-chan publishResult,
	lastSeenLSN *atomic.Int64,
	done chan<- struct{},
) {
	defer close(done)
	for curpub := range publish {
		if queueCtx.Err() != nil {
			continue
		}
		if curpub.PublishResult == nil {
			if lastSeenLSN != nil {
				shared.AtomicInt64Max(lastSeenLSN, curpub.lsn)
			}
		} else if _, err := curpub.Get(queueCtx); err != nil {
			queueErr(fmt.Errorf("[pubsub] error publishing message: %w", err))
		}
	}
}

// finishPublishing tears a batch down once every record has been handed to the
// pool. The pool is always waited for: it cannot block once the batch is
// cancelled, and publish may only be closed when nothing will send on it again.
// Returning early here used to leak the consumer goroutine, the Lua states and
// the publishers of a failed batch. Each publisher's Stop waits for its pending
// messages to be sent or to fail, up to the publish timeout after an outage, so
// they run concurrently and are not waited for past the activity's own
// cancellation. Returns the batch's error if it was cancelled.
func finishPublishing(
	ctx, queueCtx context.Context,
	pool *utils.LPool[poolResult],
	publish chan publishResult,
	topiccache *topicCache,
	consumed <-chan struct{},
) error {
	// Only fails on context cancellation, which this context cannot do.
	_ = pool.Wait(context.WithoutCancel(queueCtx))
	close(publish)
	var stopping sync.WaitGroup
	topiccache.ForEach(context.WithoutCancel(queueCtx), func(_ *pubsubpb.Topic, publisher *pubsub.Publisher) {
		stopping.Go(publisher.Stop)
	})
	stopped := make(chan struct{})
	go func() {
		stopping.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
	}
	<-consumed
	if err := context.Cause(queueCtx); err != nil {
		return fmt.Errorf("[pubsub] sync failed: %w", err)
	}
	return nil
}

func (c *PubSubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	numRecords := atomic.Int64{}
	lastSeenLSN := atomic.Int64{}
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	topiccache := c.NewTopicCache()
	publish := make(chan publishResult, 32)
	waitChan := make(chan struct{})

	queueCtx, queueErr := context.WithCancelCause(ctx)

	pool, err := c.createPool(queueCtx, req.Env, req.Script, req.FlowJobName, &topiccache, publish, queueErr)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	go consumeResults(queueCtx, queueErr, publish, &lastSeenLSN, waitChan)

	flushLoopDone := make(chan struct{})
	go func() {
		flushTimeout, err := internal.PeerDBQueueFlushTimeoutSeconds(ctx, req.Env)
		if err != nil {
			c.logger.Warn("[pubsub] failed to get flush timeout, no periodic flushing", slog.Any("error", err))
			return
		}
		ticker := time.NewTicker(flushTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-flushLoopDone:
				return
			// flush loop doesn't block processing new messages
			case <-ticker.C:
				lastSeen := lastSeenLSN.Load()
				if lastSeen > req.ConsumedOffset.Load() {
					if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{ID: lastSeen}); err != nil {
						c.logger.Warn("[pubsub] SetLastOffset error", slog.Any("error", err))
					} else {
						shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
						c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeen))
					}
				}
			}
		}
	}()

Loop:
	for {
		select {
		case record, ok := <-req.Records.GetRecords():
			if !ok {
				c.logger.Info("flushing batches because no more records")
				break Loop
			}

			pool.Run(func(ls *lua.LState) poolResult {
				lfn := ls.Env.RawGetString("onRecord")
				fn, ok := lfn.(*lua.LFunction)
				if !ok {
					queueErr(fmt.Errorf("script should define `onRecord` as function, not %v", lfn))
					return poolResult{}
				}

				ls.Push(fn)
				ls.Push(pua.LuaRecord.New(ls, record))
				err := ls.PCall(1, -1, nil)
				if err != nil {
					queueErr(fmt.Errorf("script failed: %w", err))
					return poolResult{}
				}

				args := ls.GetTop()
				results := make([]PubSubMessage, 0, args)
				for i := range args {
					msg, err := lvalueToPubSubMessage(ls, ls.Get(i-args))
					if err != nil {
						queueErr(fmt.Errorf("[pubsub] error creating message: %w", err))
						return poolResult{}
					}
					if msg.Message != nil {
						if msg.Topic == "" {
							msg.Topic = record.GetDestinationTableName()
						}
						results = append(results, msg)
						record.PopulateCountMap(tableNameRowsMapping)
					}
				}
				ls.SetTop(0)
				numRecords.Add(1)
				return poolResult{
					messages: results,
					lsn:      record.GetCheckpointID(),
				}
			})

		case <-queueCtx.Done():
			break Loop
		}
	}

	close(flushLoopDone)
	if err := finishPublishing(ctx, queueCtx, pool, publish, &topiccache, waitChan); err != nil {
		return nil, err
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
		return nil, fmt.Errorf("[pubsub] FinishBatch error: %w", err)
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:   req.SyncBatchID,
		LastSyncedCheckpoint: lastCheckpoint,
		NumRecordsSynced:     numRecords.Load(),
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}

func (c *PubSubConnector) NewTopicCache() topicCache {
	return topicCache{
		client: c.client,
		cache:  make(map[string]topicCacheValue),
	}
}

type topicCacheValue struct {
	Topic     *pubsubpb.Topic
	Publisher *pubsub.Publisher
}

type topicCache struct {
	client *pubsub.Client
	cache  map[string]topicCacheValue
	lock   sync.RWMutex
}

func (tc *topicCache) GetOrSet(topicName string, f func() (*pubsubpb.Topic, error)) (topicCacheValue, error) {
	tc.lock.RLock()
	value, ok := tc.cache[topicName]
	tc.lock.RUnlock()
	if ok {
		return value, nil
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	// check cache again, in case of write race
	if client, ok := tc.cache[topicName]; ok {
		return client, nil
	}
	topic, err := f()
	if err != nil {
		return topicCacheValue{}, err
	}
	value = topicCacheValue{
		Topic:     topic,
		Publisher: tc.client.Publisher(topic.Name),
	}
	tc.cache[topicName] = value
	return value, nil
}

func (tc *topicCache) ForEach(ctx context.Context, f func(topic *pubsubpb.Topic, publisher *pubsub.Publisher)) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	for _, value := range tc.cache {
		if ctx.Err() != nil {
			return
		}
		f(value.Topic, value.Publisher)
	}
}
