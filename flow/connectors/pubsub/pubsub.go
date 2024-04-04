package connpubsub

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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

func lvalueToPubSubMessage(ls *lua.LState, value lua.LValue) (string, *pubsub.Message, error) {
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
			return "", nil, fmt.Errorf("invalid key, %w", err)
		}
		value, err := utils.LVAsReadOnlyBytes(ls, ls.GetField(v, "value"))
		if err != nil {
			return "", nil, fmt.Errorf("invalid value, %w", err)
		}
		topic, err = utils.LVAsStringOrNil(ls, ls.GetField(v, "topic"))
		if err != nil {
			return "", nil, fmt.Errorf("invalid topic, %w", err)
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
			return "", nil, fmt.Errorf("invalid headers, must be nil or table: %s", lheaders)
		}
	case *lua.LNilType:
	default:
		return "", nil, fmt.Errorf("script returned invalid value: %s", value)
	}
	return topic, msg, nil
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

func (c *PubSubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	numRecords := int64(0)
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)

	ls, err := utils.LoadScript(ctx, req.Script, func(ls *lua.LState) int {
		top := ls.GetTop()
		ss := make([]string, top)
		for i := range top {
			ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
		}
		_ = c.LogFlowInfo(ctx, req.FlowJobName, strings.Join(ss, "\t"))
		return 0
	})
	if err != nil {
		return nil, err
	}
	defer ls.Close()
	if req.Script == "" {
		ls.Env.RawSetString("onRecord", ls.NewFunction(utils.DefaultOnRecord))
	}

	lfn := ls.Env.RawGetString("onRecord")
	fn, ok := lfn.(*lua.LFunction)
	if !ok {
		return nil, fmt.Errorf("script should define `onRecord` as function, not %s", lfn)
	}

	var wg sync.WaitGroup
	wgCtx, wgErr := context.WithCancelCause(ctx)
	publish := make(chan *pubsub.PublishResult, 60)
	go func() {
		var curpub *pubsub.PublishResult
		for {
			select {
			case curpub, ok = <-publish:
				if !ok {
					return
				}
			case <-ctx.Done():
				wgErr(ctx.Err())
				return
			}
			_, err := curpub.Get(ctx)
			if err != nil {
				wgErr(err)
				return
			}
			wg.Done()
		}
	}()

	topiccache := topicCache{cache: make(map[string]*pubsub.Topic)}
	lastSeenLSN := atomic.Int64{}
	flushLoopDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(peerdbenv.PeerDBQueueFlushTimeoutSeconds())
		defer ticker.Stop()

		lastUpdatedOffset := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-flushLoopDone:
				return
			// flush loop doesn't block processing new messages
			case <-ticker.C:
				lastSeen := lastSeenLSN.Load()
				if lastSeen > lastUpdatedOffset {
					if err := c.SetLastOffset(ctx, req.FlowJobName, lastSeen); err != nil {
						c.logger.Warn("[kafka] SetLastOffset error", slog.Any("error", err))
					} else {
						lastUpdatedOffset = lastSeen
						c.logger.Info("processBatch", slog.Int64("updated last offset", lastUpdatedOffset))
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
			ls.Push(fn)
			ls.Push(pua.LuaRecord.New(ls, record))
			err := ls.PCall(1, -1, nil)
			if err != nil {
				return nil, fmt.Errorf("script failed: %w", err)
			}
			args := ls.GetTop()
			for i := range args {
				topic, msg, err := lvalueToPubSubMessage(ls, ls.Get(i-args))
				if err != nil {
					return nil, err
				}
				if msg != nil {
					if topic == "" {
						topic = record.GetDestinationTableName()
					}
					topicClient, err := topiccache.GetOrSet(topic, func() (*pubsub.Topic, error) {
						topicClient := c.client.Topic(topic)
						exists, err := topicClient.Exists(wgCtx)
						if err != nil {
							return nil, fmt.Errorf("error checking if topic exists: %w", err)
						}
						if !exists {
							topicClient, err = c.client.CreateTopic(wgCtx, topic)
							if err != nil {
								return nil, fmt.Errorf("error creating topic: %w", err)
							}
						}
						return topicClient, nil
					})
					if err != nil {
						return nil, err
					}

					pubresult := topicClient.Publish(ctx, msg)
					wg.Add(1)
					publish <- pubresult
					record.PopulateCountMap(tableNameRowsMapping)
				}
			}
			ls.SetTop(0)
			numRecords += 1
			shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())

		case <-wgCtx.Done():
			return nil, wgCtx.Err()
		}
	}

	close(flushLoopDone)
	close(publish)
	topiccache.Stop(wgCtx)
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()
	select {
	case <-wgCtx.Done():
		return nil, wgCtx.Err()
	case <-waitChan:
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err = c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       numRecords,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}
