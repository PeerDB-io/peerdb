package conneventhub

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	lua "github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

type EventHubConnector struct {
	*metadataStore.PostgresMetadata
	config     *protos.EventHubGroupConfig
	creds      *azidentity.DefaultAzureCredential
	hubManager *EventHubManager
	logger     log.Logger
}

// NewEventHubConnector creates a new EventHubConnector.
func NewEventHubConnector(
	ctx context.Context,
	config *protos.EventHubGroupConfig,
) (*EventHubConnector, error) {
	logger := shared.LoggerFromCtx(ctx)
	defaultAzureCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		logger.Error("failed to get default azure credentials", "error", err)
		return nil, err
	}

	hubManager := NewEventHubManager(defaultAzureCreds, config)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	return &EventHubConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		creds:            defaultAzureCreds,
		hubManager:       hubManager,
		logger:           logger,
	}, nil
}

func (c *EventHubConnector) Close() error {
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		err := c.hubManager.Close(timeout)
		if err != nil {
			c.logger.Error("failed to close event hub manager", slog.Any("error", err))
			return err
		}
	}

	return nil
}

func (c *EventHubConnector) ConnectionActive(ctx context.Context) error {
	return nil
}

func (c *EventHubConnector) NeedsSetupMetadataTables(_ context.Context) bool {
	return false
}

func (c *EventHubConnector) SetupMetadataTables(_ context.Context) error {
	return nil
}

func lvalueToEventData(ls *lua.LState, value lua.LValue) (ScopedEventhubData, error) {
	var scoped ScopedEventhubData
	switch v := value.(type) {
	case lua.LString:
		scoped.Data = &azeventhubs.EventData{
			Body: shared.UnsafeFastStringToReadOnlyBytes(string(v)),
		}
	case *lua.LTable:
		value, err := utils.LVAsReadOnlyBytes(ls, ls.GetField(v, "value"))
		if err != nil {
			return scoped, fmt.Errorf("invalid value, %w", err)
		}
		destination, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "destination"))
		if err != nil {
			return scoped, fmt.Errorf("invalid destination, %w", err)
		}
		if destination != "" {
			scoped.Hub, err = NewScopedEventhub(destination)
			if err != nil {
				return scoped, fmt.Errorf("invalid topic, %w", err)
			}
		}
		namespace, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "namespace"))
		if err != nil {
			return scoped, fmt.Errorf("invalid namespace, %w", err)
		}
		if namespace != "" {
			scoped.Hub.NamespaceName = namespace
		}
		partCol, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "partitionColumn"))
		if err != nil {
			return scoped, fmt.Errorf("invalid partitionColumn, %w", err)
		}
		if partCol != "" {
			scoped.Hub.PartitionKeyColumn = partCol
		}
		key, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "key"))
		if err != nil {
			return scoped, fmt.Errorf("invalid key, %w", err)
		}
		if key != "" {
			scoped.Hub.PartitionKeyValue = key
		}
		hub, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "hub"))
		if err != nil {
			return scoped, fmt.Errorf("invalid hub, %w", err)
		}
		if hub != "" {
			scoped.Hub.Eventhub = hub
		}
		contentType, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "contentType"))
		if err != nil {
			return scoped, fmt.Errorf("invalid contentType, %w", err)
		}
		contentTypePtr := &contentType
		if contentType == "" {
			contentTypePtr = nil
		}
		messageId, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "messageId"))
		if err != nil {
			return scoped, fmt.Errorf("invalid messageId, %w", err)
		}
		messageIdPtr := &messageId
		if messageId == "" {
			messageIdPtr = nil
		}
		scoped.Data = &azeventhubs.EventData{
			Body:        value,
			ContentType: contentTypePtr,
			MessageID:   messageIdPtr,
		}
		lheaders := ls.GetField(v, "headers")
		if headers, ok := lheaders.(*lua.LTable); ok {
			scoped.Data.Properties = make(map[string]any)
			headers.ForEach(func(k, v lua.LValue) {
				scoped.Data.Properties[k.String()] = v
			})
		} else if lua.LVAsBool(lheaders) {
			return scoped, fmt.Errorf("invalid headers, must be nil or table: %s", lheaders)
		}
	case *lua.LNilType:
	default:
		return scoped, fmt.Errorf("script returned invalid value: %s", value)
	}
	return scoped, nil
}

type ScopedEventhubData struct {
	Data *azeventhubs.EventData
	Hub  ScopedEventhub
}

// returns the number of records synced
func (c *EventHubConnector) processBatch(
	ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
) (uint32, error) {
	batchPerTopic := NewHubBatches(c.hubManager)
	toJSONOpts := model.NewToJSONOptions(c.config.UnnestColumns, false)

	flushTimeout, err := peerdbenv.PeerDBQueueFlushTimeoutSeconds(ctx, req.Env)
	if err != nil {
		return 0, fmt.Errorf("failed to get flush timeout: %w", err)
	}
	ticker := time.NewTicker(flushTimeout)
	defer ticker.Stop()

	lastSeenLSN := int64(0)

	numRecords := atomic.Uint32{}

	var ls *lua.LState
	var fn *lua.LFunction
	if req.Script != "" {
		var err error
		ls, err = utils.LoadScript(ctx, req.Script, utils.LuaPrintFn(func(s string) {
			_ = c.LogFlowInfo(ctx, req.FlowJobName, s)
		}))
		if err != nil {
			return 0, err
		}
		defer ls.Close()

		lfn := ls.Env.RawGetString("onRecord")
		var ok bool
		fn, ok = lfn.(*lua.LFunction)
		if !ok {
			return 0, fmt.Errorf("script should define `onRecord` as function, not %s", lfn)
		}
	}

	for {
		select {
		case record, ok := <-req.Records.GetRecords():
			if !ok {
				c.logger.Info("flushing batches because no more records")
				err := batchPerTopic.flushAllBatches(ctx, req.FlowJobName)
				if err != nil {
					return 0, err
				}

				currNumRecords := numRecords.Load()

				c.logger.Info("processBatch", slog.Int("Total records sent to event hub", int(currNumRecords)))
				return currNumRecords, nil
			}

			recordLSN := record.GetCheckpointID()
			if recordLSN > lastSeenLSN {
				lastSeenLSN = recordLSN
			}

			var events []ScopedEventhubData
			destinationString := record.GetDestinationTableName()
			if fn != nil {
				ls.Push(fn)
				ls.Push(pua.LuaRecord.New(ls, record))
				err := ls.PCall(1, -1, nil)
				if err != nil {
					return 0, fmt.Errorf("script failed: %w", err)
				}

				args := ls.GetTop()
				for i := range args {
					scoped, err := lvalueToEventData(ls, ls.Get(i-args))
					if err != nil {
						return 0, err
					}

					if scoped.Data != nil {
						if scoped.Hub.NamespaceName == "" {
							scoped.Hub, err = NewScopedEventhub(destinationString)
							if err != nil {
								c.logger.Error("failed to get topic name", slog.Any("error", err))
								return 0, err
							}
						}
						events = append(events, scoped)
					}
				}
				ls.SetTop(0)
			} else {
				json, err := record.GetItems().ToJSONWithOptions(toJSONOpts)
				if err != nil {
					c.logger.Info("failed to convert record to json", slog.Any("error", err))
					return 0, err
				}
				scopedHub, err := NewScopedEventhub(destinationString)
				if err != nil {
					c.logger.Error("failed to get topic name", slog.Any("error", err))
					return 0, err
				}
				events = []ScopedEventhubData{{Hub: scopedHub, Data: &azeventhubs.EventData{Body: []byte(json)}}}
			}

			for _, event := range events {
				ehConfig, ok := c.hubManager.namespaceToEventhubMap.Get(event.Hub.NamespaceName)
				if !ok {
					c.logger.Error("failed to get eventhub config", slog.String("namespace", event.Hub.NamespaceName))
					return 0, fmt.Errorf("failed to get eventhub config %s", event.Hub.NamespaceName)
				}

				// Scoped eventhub is of the form peer_name.eventhub_name.partition_column
				// partition_column is the column in the table that is used to determine
				// the partition key for the eventhub.
				partitionKey := event.Hub.PartitionKeyValue
				if partitionKey == "" {
					partitionColumn := event.Hub.PartitionKeyColumn
					partitionValue := record.GetItems().GetColumnValue(partitionColumn).Value()
					if partitionValue != nil {
						partitionKey = fmt.Sprint(partitionValue)
					}

					partitionKey = utils.HashedPartitionKey(partitionKey, ehConfig.PartitionCount)
					event.Hub.PartitionKeyValue = partitionKey
				}
				err := batchPerTopic.AddEvent(ctx, event.Hub, event.Data, false)
				if err != nil {
					c.logger.Error("failed to add event to batch", slog.Any("error", err))
					return 0, err
				}
			}

			curNumRecords := numRecords.Add(1)
			if curNumRecords%10000 == 0 {
				c.logger.Info("processBatch", slog.Int("number of records processed for sending", int(curNumRecords)))
			}

		case <-ctx.Done():
			return 0, fmt.Errorf("[eventhub] context cancelled %w", ctx.Err())

		case <-ticker.C:
			err := batchPerTopic.flushAllBatches(ctx, req.FlowJobName)
			if err != nil {
				return 0, err
			} else if lastSeenLSN > req.ConsumedOffset.Load() {
				if err := c.SetLastOffset(ctx, req.FlowJobName, lastSeenLSN); err != nil {
					c.logger.Warn("[eventhubs] SetLastOffset error", slog.Any("error", err))
				} else {
					shared.AtomicInt64Max(req.ConsumedOffset, lastSeenLSN)
					c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeenLSN))
				}
			}
		}
	}
}

func (c *EventHubConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	numRecords, err := c.processBatch(ctx, req)
	if err != nil {
		c.logger.Error("failed to process batch", slog.Any("error", err))
		return nil, err
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err = c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to increment id", slog.Any("error", err))
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		TableNameRowsMapping:   make(map[string]*model.RecordTypeCounts),
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *EventHubConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	// create topics for each table
	// key is the source table and value is the "eh_peer.eh_topic" that ought to be used.
	tableMap := req.GetTableNameMapping()

	for _, destinationTable := range tableMap {
		// parse peer name and topic name.
		name, err := NewScopedEventhub(destinationTable)
		if err != nil {
			c.logger.Error("failed to parse scoped eventhub name",
				slog.Any("error", err), slog.String("destinationTable", destinationTable))
			return nil, err
		}

		err = c.hubManager.EnsureEventHubExists(ctx, name)
		if err != nil {
			c.logger.Error("failed to ensure eventhub exists",
				slog.Any("error", err), slog.String("destinationTable", destinationTable))
			return nil, err
		}
	}

	return &protos.CreateRawTableOutput{
		TableIdentifier: "n/a",
	}, nil
}

func (c *EventHubConnector) ReplayTableSchemaDeltas(_ context.Context, _ map[string]string,
	flowJobName string, schemaDeltas []*protos.TableSchemaDelta,
) error {
	c.logger.Info("ReplayTableSchemaDeltas for event hub is a no-op")
	return nil
}
