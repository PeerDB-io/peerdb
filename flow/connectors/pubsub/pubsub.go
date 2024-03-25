package connpubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/pubsub"
	lua "github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PubSubConnector struct {
	client     *pubsub.Client
	pgMetadata *metadataStore.PostgresMetadataStore
	logger     log.Logger
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

	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		return nil, err
	}

	return &PubSubConnector{
		client:     client,
		pgMetadata: pgMetadata,
		logger:     logger.LoggerFromCtx(ctx),
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

func (c *PubSubConnector) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(ctx, jobName)
}

func (c *PubSubConnector) GetLastOffset(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(ctx, jobName)
}

func (c *PubSubConnector) SetLastOffset(ctx context.Context, jobName string, offset int64) error {
	return c.pgMetadata.UpdateLastOffset(ctx, jobName, offset)
}

func (c *PubSubConnector) NeedsSetupMetadataTables(_ context.Context) bool {
	return false
}

func (c *PubSubConnector) SetupMetadataTables(_ context.Context) error {
	return nil
}

func (c *PubSubConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	return nil
}

func (c *PubSubConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	return c.pgMetadata.DropMetadata(ctx, jobName)
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

func (c *PubSubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	numRecords := int64(0)
	tableNameRowsMapping := make(map[string]*model.RecordTypeCounts)

	ls, err := utils.LoadScript(ctx, req.Script, func(ls *lua.LState) int {
		top := ls.GetTop()
		ss := make([]string, top)
		for i := range top {
			ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
		}
		_ = c.pgMetadata.LogFlowInfo(ctx, req.FlowJobName, strings.Join(ss, "\t"))
		return 0
	})
	if err != nil {
		return nil, err
	}
	defer ls.Close()

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

	topiccache := make(map[string]*pubsub.Topic)
	for record := range req.Records.GetRecords() {
		if err := ctx.Err(); err != nil {
			return nil, err
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
				topicClient, ok := topiccache[topic]
				if !ok {
					topicClient = c.client.Topic(topic)
					exists, err := topicClient.Exists(ctx)
					if err != nil {
						return nil, fmt.Errorf("error checking if topic exists: %w", err)
					}
					if !exists {
						topicClient, err = c.client.CreateTopic(ctx, topic)
						if err != nil {
							return nil, fmt.Errorf("error creating topic: %w", err)
						}
					}
					topiccache[topic] = topicClient
				}

				pubresult := topicClient.Publish(ctx, msg)
				wg.Add(1)
				publish <- pubresult
				record.PopulateCountMap(tableNameRowsMapping)
			}
		}
		numRecords += 1
		ls.SetTop(0)
	}

	close(publish)
	for _, topicClient := range topiccache {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		topicClient.Stop()
	}
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
	err = c.pgMetadata.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
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
