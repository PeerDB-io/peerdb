package connopensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	actionIndex  = "index"
	actionDelete = "delete"
)

type OpensearchConnector struct {
	*metadataStore.PostgresMetadata
	client    *opensearch.Client
	apiClient *opensearchapi.Client
	logger    log.Logger
}

func NewOpensearchConnector(ctx context.Context,
	config *protos.OpensearchConfig,
) (*OpensearchConnector, error) {
	osCfg := opensearch.Config{
		Addresses: config.Addresses,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13},
		},
	}
	if config.AuthType == protos.ElasticsearchAuthType_BASIC {
		osCfg.Username = *config.Username
		osCfg.Password = *config.Password
	} else if config.AuthType == protos.ElasticsearchAuthType_APIKEY {
		// TODO: Add API Key support
	}

	osClient, err := opensearch.NewClient(osCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating opensearch connector: %w", err)
	}
	apiClient, err := opensearchapi.NewClient(opensearchapi.Config{Client: osCfg})
	if err != nil {
		return nil, fmt.Errorf("error creating opensearch API client: %w", err)
	}
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &OpensearchConnector{
		PostgresMetadata: pgMetadata,
		client:           osClient,
		apiClient:        apiClient,
		logger:           internal.LoggerFromCtx(ctx),
	}, nil
}

func (osc *OpensearchConnector) ConnectionActive(ctx context.Context) error {
	err := osc.client.DiscoverNodes()
	if err != nil {
		return fmt.Errorf("failed to check if opensearch peer is active: %w", err)
	}
	return nil
}

func (osc *OpensearchConnector) Close() error {
	// stateless connector
	return nil
}

// ES is queue-like, no raw table staging needed
func (osc *OpensearchConnector) CreateRawTable(ctx context.Context,
	req *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

// we handle schema changes by not handling them since no mapping is being enforced right now
func (osc *OpensearchConnector) ReplayTableSchemaDeltas(ctx context.Context, env map[string]string,
	flowJobName string, _ []*protos.TableMapping, schemaDeltas []*protos.TableSchemaDelta,
) error {
	return nil
}

func recordItemsProcessor(items model.RecordItems) ([]byte, error) {
	qRecordJsonMap := make(map[string]any)

	for key, val := range items.ColToVal {
		if r, ok := val.(types.QValueJSON); ok { // JSON is stored as a string, fix that
			qRecordJsonMap[key] = json.RawMessage(
				shared.UnsafeFastStringToReadOnlyBytes(r.Val))
		} else {
			qRecordJsonMap[key] = val.Value()
		}
	}

	return json.Marshal(qRecordJsonMap)
}

func (osc *OpensearchConnector) SyncRecords(ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	var lastSeenLSN atomic.Int64
	var numRecords int64

	// no I don't like this either
	osBulkIndexerCache := make(map[string]opensearchutil.BulkIndexer)
	bulkIndexersHaveShutdown := false
	// true if we saw errors while closing
	cacheCloser := func() bool {
		closeHasErrors := false
		if !bulkIndexersHaveShutdown {
			for osBulkIndexer := range maps.Values(osBulkIndexerCache) {
				err := osBulkIndexer.Close(context.Background())
				if err != nil {
					osc.logger.Error("[os] failed to close bulk indexer", slog.Any("error", err))
					closeHasErrors = true
				}
				numRecords += int64(osBulkIndexer.Stats().NumFlushed)
			}
			bulkIndexersHaveShutdown = true
		}
		return closeHasErrors
	}
	defer cacheCloser()

	flushLoopDone := make(chan struct{})
	go func() {
		flushTimeout, err := internal.PeerDBQueueFlushTimeoutSeconds(ctx, req.Env)
		if err != nil {
			osc.logger.Warn("[opensearch] failed to get flush timeout, no periodic flushing", slog.Any("error", err))
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
			case <-ticker.C:
				lastSeen := lastSeenLSN.Load()
				if lastSeen > req.ConsumedOffset.Load() {
					if err := osc.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{ID: lastSeen}); err != nil {
						osc.logger.Warn("[os] SetLastOffset error", slog.Any("error", err))
					} else {
						shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
						osc.logger.Info("processBatch", slog.Int64("updated last offset", lastSeen))
					}
				}
			}
		}
	}()

	var docId string
	var bulkIndexFatalError error
	var bulkIndexErrors []error
	var bulkIndexOnFailureMutex sync.Mutex

	for record := range req.Records.GetRecords() {
		if _, ok := record.(*model.MessageRecord[model.RecordItems]); ok {
			continue
		}

		var bodyBytes []byte
		var err error
		action := actionIndex

		switch record.(type) {
		case *model.InsertRecord[model.RecordItems], *model.UpdateRecord[model.RecordItems]:
			bodyBytes, err = recordItemsProcessor(record.GetItems())
			if err != nil {
				osc.logger.Error("[os] failed to json.Marshal record", slog.Any("error", err))
				return nil, fmt.Errorf("[os] failed to json.Marshal record: %w", err)
			}
		case *model.DeleteRecord[model.RecordItems]:
			action = actionDelete
			// no need to supply the document since we are deleting
			bodyBytes = nil
		}

		bulkIndexer, ok := osBulkIndexerCache[record.GetDestinationTableName()]
		if !ok {
			bulkIndexer, err = opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
				Index:  record.GetDestinationTableName(),
				Client: osc.apiClient,
				// can't really ascertain how many tables present to provide a reasonable value
				NumWorkers:    1,
				FlushInterval: 10 * time.Second,
			})
			if err != nil {
				osc.logger.Error("[os] failed to initialize bulk indexer", slog.Any("error", err))
				return nil, fmt.Errorf("[os] failed to initialize bulk indexer: %w", err)
			}
			osBulkIndexerCache[record.GetDestinationTableName()] = bulkIndexer
		}

		if len(req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns) == 1 {
			qValue, err := record.GetItems().GetValueByColName(
				req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns[0])
			if err != nil {
				osc.logger.Error("[os] failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("[os] failed to process record: %w", err)
			}
			docId = fmt.Sprint(qValue.Value())
		} else {
			tablePkey, err := model.RecToTablePKey(req.TableNameSchemaMapping, record)
			if err != nil {
				osc.logger.Error("[os] failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("[os] failed to process record: %w", err)
			}
			docId = base64.RawURLEncoding.EncodeToString(tablePkey.PkeyColVal[:])
		}

		if err := bulkIndexer.Add(ctx, opensearchutil.BulkIndexerItem{
			Action:     action,
			DocumentID: docId,
			Body:       bytes.NewReader(bodyBytes),
			OnSuccess: func(_ context.Context, _ opensearchutil.BulkIndexerItem, _ opensearchapi.BulkRespItem) {
				shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())
				record.PopulateCountMap(tableNameRowsMapping)
			},
			// OnFailure is called for each failed operation, log and let parent handle
			OnFailure: func(ctx context.Context, item opensearchutil.BulkIndexerItem,
				res opensearchapi.BulkRespItem, err error,
			) {
				// attempt to delete a record that wasn't present, possible from no initial load
				if item.Action == actionDelete && res.Status == 404 {
					return
				}
				bulkIndexOnFailureMutex.Lock()
				defer bulkIndexOnFailureMutex.Unlock()
				if err != nil {
					bulkIndexErrors = append(bulkIndexErrors, err)
				} else {
					causeString := ""
					if res.Error.Cause.Type != "" || res.Error.Cause.Reason != "" {
						causeString = fmt.Sprintf("(caused by type:%s reason:%s)", res.Error.Cause.Type, res.Error.Cause.Reason)
					}
					cbErr := fmt.Errorf("id:%s action:%s type:%s reason:%s %s", item.DocumentID, item.Action, res.Error.Type,
						res.Error.Reason, causeString)
					bulkIndexErrors = append(bulkIndexErrors, cbErr)
					if res.Error.Type == "illegal_argument_exception" {
						bulkIndexFatalError = cbErr
					}
				}
			},
		}); err != nil {
			osc.logger.Error("[os] failed to add record to bulk indexer", slog.Any("error", err))
			return nil, fmt.Errorf("[os] failed to add record to bulk indexer: %w", err)
		}
		if bulkIndexFatalError != nil {
			osc.logger.Error("[os] fatal error while indexing record", slog.Any("error", bulkIndexFatalError))
			return nil, fmt.Errorf("[os] fatal error while indexing record: %w", bulkIndexFatalError)
		}
	}
	// "Receive on a closed channel yields the zero value after all elements in the channel are received."
	close(flushLoopDone)

	if cacheCloser() {
		osc.logger.Error("[os] failed to close bulk indexer(s)")
		return nil, errors.New("[os] failed to close bulk indexer(s)")
	}
	if len(bulkIndexErrors) > 0 {
		for _, err := range bulkIndexErrors {
			osc.logger.Error("[os] failed to index record", slog.Any("err", err))
		}
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := osc.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:   req.SyncBatchID,
		LastSyncedCheckpoint: lastCheckpoint,
		NumRecordsSynced:     numRecords,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}
