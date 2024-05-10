package connpubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	lua "github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PubSubConnector struct {
	*metadataStore.PostgresMetadata
	client *pubsub.Client
	logger log.Logger
}

func NewPubSubConnector(
	ctx context.Context,
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
		logger:           logger.LoggerFromCtx(ctx),
	}, nil
}

func (c *PubSubConnector) Close() error {
	if c != nil {
		c.client.Close()
	}
	return nil
}

func (c *PubSubConnector) ConnectionActive(ctx context.Context) error {
	topic := c.client.Topic("test")
	_, err := topic.Exists(ctx)
	return err
}

func (c *PubSubConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

func (c *PubSubConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	return nil
}

type PubSubMessage struct {
	*pubsub.Message
	Topic string
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
	script string,
	flowJobName string,
	topiccache *topicCache,
	publish chan<- *pubsub.PublishResult,
	queueErr func(error),
) (*utils.LPool[[]PubSubMessage], error) {
	return utils.LuaPool(func() (*lua.LState, error) {
		ls, err := utils.LoadScript(ctx, script, utils.LuaPrintFn(func(s string) {
			_ = c.LogFlowInfo(ctx, flowJobName, s)
		}))
		if err != nil {
			return nil, err
		}
		if script == "" {
			ls.Env.RawSetString("onRecord", ls.NewFunction(utils.DefaultOnRecord))
		}
		return ls, nil
	}, func(messages []PubSubMessage) {
		for _, message := range messages {
			topicClient, err := topiccache.GetOrSet(message.Topic, func() (*pubsub.Topic, error) {
				topicClient := c.client.Topic(message.Topic)
				exists, err := topicClient.Exists(ctx)
				if err != nil {
					return nil, fmt.Errorf("error checking if topic exists: %w", err)
				}
				if !exists {
					topicClient, err = c.client.CreateTopic(ctx, message.Topic)
					if err != nil {
						return nil, fmt.Errorf("error creating topic: %w", err)
					}
				}
				return topicClient, nil
			})
			if err != nil {
				queueErr(err)
				return
			}

			publish <- topicClient.Publish(ctx, message.Message)
		}
	})
}

type topicCache struct {
	cache map[string]*pubsub.Topic
	lock  sync.RWMutex
}

func (tc *topicCache) forEach(ctx context.Context, f func(topic *pubsub.Topic)) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	for _, topicClient := range tc.cache {
		if ctx.Err() != nil {
			return
		}
		f(topicClient)
	}
}

func (tc *topicCache) Flush(ctx context.Context) {
	tc.forEach(ctx, func(topic *pubsub.Topic) {
		topic.Flush()
	})
}

func (tc *topicCache) Stop(ctx context.Context) {
	tc.forEach(ctx, func(topic *pubsub.Topic) {
		topic.Stop()
	})
}

func (tc *topicCache) GetOrSet(topic string, f func() (*pubsub.Topic, error)) (*pubsub.Topic, error) {
	tc.lock.RLock()
	client, ok := tc.cache[topic]
	tc.lock.RUnlock()
	if ok {
		return client, nil
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	// check cache again, in case of write race
	if client, ok := tc.cache[topic]; ok {
		return client, nil
	}
	client, err := f()
	if err != nil {
		return nil, err
	}
	tc.cache[topic] = client
	return client, nil
}

func (c *PubSubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	numRecords := atomic.Int64{}
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	topiccache := topicCache{cache: make(map[string]*pubsub.Topic)}
	publish := make(chan *pubsub.PublishResult, 32)
	waitChan := make(chan struct{})

	queueCtx, queueErr := context.WithCancelCause(ctx)
	pool, err := c.createPool(queueCtx, req.Script, req.FlowJobName, &topiccache, publish, queueErr)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	go func() {
		for curpub := range publish {
			if _, err := curpub.Get(ctx); err != nil {
				queueErr(err)
				break
			}
		}
		close(waitChan)
	}()

	lastSeenLSN := atomic.Int64{}
	flushLoopDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(peerdbenv.PeerDBQueueFlushTimeoutSeconds())
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
					if err := c.SetLastOffset(ctx, req.FlowJobName, lastSeen); err != nil {
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

			pool.Run(func(ls *lua.LState) []PubSubMessage {
				lfn := ls.Env.RawGetString("onRecord")
				fn, ok := lfn.(*lua.LFunction)
				if !ok {
					queueErr(fmt.Errorf("script should define `onRecord` as function, not %s", lfn))
					return nil
				}

				ls.Push(fn)
				ls.Push(pua.LuaRecord.New(ls, record))
				err := ls.PCall(1, -1, nil)
				if err != nil {
					queueErr(fmt.Errorf("script failed: %w", err))
					return nil
				}

				args := ls.GetTop()
				results := make([]PubSubMessage, 0, args)
				for i := range args {
					msg, err := lvalueToPubSubMessage(ls, ls.Get(i-args))
					if err != nil {
						queueErr(err)
						return nil
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
				shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())
				return results
			})

		case <-queueCtx.Done():
			break Loop
		}
	}

	close(flushLoopDone)
	if err := pool.Wait(queueCtx); err != nil {
		return nil, err
	}
	close(publish)
	topiccache.Stop(queueCtx)
	select {
	case <-queueCtx.Done():
		return nil, queueCtx.Err()
	case <-waitChan:
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       numRecords.Load(),
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}
