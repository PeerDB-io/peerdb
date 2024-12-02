package connelasticsearch

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

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	actionIndex  = "index"
	actionDelete = "delete"
)

type ElasticsearchConnector struct {
	*metadataStore.PostgresMetadata
	client *elasticsearch.Client
	logger log.Logger
}

func NewElasticsearchConnector(ctx context.Context,
	config *protos.ElasticsearchConfig,
) (*ElasticsearchConnector, error) {
	esCfg := &elasticsearch.Config{
		Addresses: config.Addresses,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS13,
			},
		},
	}
	if config.AuthType == protos.ElasticsearchAuthType_BASIC {
		esCfg.Username = *config.Username
		esCfg.Password = *config.Password
	} else if config.AuthType == protos.ElasticsearchAuthType_APIKEY {
		esCfg.APIKey = *config.ApiKey
	}

	esClient, err := elasticsearch.NewClient(*esCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating elasticsearch connector: %w", err)
	}
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &ElasticsearchConnector{
		PostgresMetadata: pgMetadata,
		client:           esClient,
		logger:           shared.LoggerFromCtx(ctx),
	}, nil
}

func (esc *ElasticsearchConnector) ConnectionActive(ctx context.Context) error {
	err := esc.client.DiscoverNodes()
	if err != nil {
		return fmt.Errorf("failed to check if elasticsearch peer is active: %w", err)
	}
	return nil
}

func (esc *ElasticsearchConnector) Close() error {
	// stateless connector
	return nil
}

// ES is queue-like, no raw table staging needed
func (esc *ElasticsearchConnector) CreateRawTable(ctx context.Context,
	req *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

// we handle schema changes by not handling them since no mapping is being enforced right now
func (esc *ElasticsearchConnector) ReplayTableSchemaDeltas(ctx context.Context, env map[string]string,
	flowJobName string, schemaDeltas []*protos.TableSchemaDelta,
) error {
	return nil
}

func recordItemsProcessor(items model.RecordItems) ([]byte, error) {
	qRecordJsonMap := make(map[string]any)

	for key, val := range items.ColToVal {
		if r, ok := val.(qvalue.QValueJSON); ok { // JSON is stored as a string, fix that
			qRecordJsonMap[key] = json.RawMessage(
				shared.UnsafeFastStringToReadOnlyBytes(r.Val))
		} else {
			qRecordJsonMap[key] = val.Value()
		}
	}

	return json.Marshal(qRecordJsonMap)
}

func (esc *ElasticsearchConnector) SyncRecords(ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	var lastSeenLSN atomic.Int64
	var numRecords int64

	// no I don't like this either
	esBulkIndexerCache := make(map[string]esutil.BulkIndexer)
	bulkIndexersHaveShutdown := false
	// true if we saw errors while closing
	cacheCloser := func() bool {
		closeHasErrors := false
		if !bulkIndexersHaveShutdown {
			for esBulkIndexer := range maps.Values(esBulkIndexerCache) {
				err := esBulkIndexer.Close(context.Background())
				if err != nil {
					esc.logger.Error("[es] failed to close bulk indexer", slog.Any("error", err))
					closeHasErrors = true
				}
				numRecords += int64(esBulkIndexer.Stats().NumFlushed)
			}
			bulkIndexersHaveShutdown = true
		}
		return closeHasErrors
	}
	defer cacheCloser()

	flushLoopDone := make(chan struct{})
	go func() {
		flushTimeout, err := peerdbenv.PeerDBQueueFlushTimeoutSeconds(ctx, req.Env)
		if err != nil {
			esc.logger.Warn("[elasticsearch] failed to get flush timeout, no periodic flushing", slog.Any("error", err))
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
					if err := esc.SetLastOffset(ctx, req.FlowJobName, lastSeen); err != nil {
						esc.logger.Warn("[es] SetLastOffset error", slog.Any("error", err))
					} else {
						shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
						esc.logger.Info("processBatch", slog.Int64("updated last offset", lastSeen))
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
				esc.logger.Error("[es] failed to json.Marshal record", slog.Any("error", err))
				return nil, fmt.Errorf("[es] failed to json.Marshal record: %w", err)
			}
		case *model.DeleteRecord[model.RecordItems]:
			action = actionDelete
			// no need to supply the document since we are deleting
			bodyBytes = nil
		}

		bulkIndexer, ok := esBulkIndexerCache[record.GetDestinationTableName()]
		if !ok {
			bulkIndexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
				Index:  record.GetDestinationTableName(),
				Client: esc.client,
				// can't really ascertain how many tables present to provide a reasonable value
				NumWorkers:    1,
				FlushInterval: 10 * time.Second,
			})
			if err != nil {
				esc.logger.Error("[es] failed to initialize bulk indexer", slog.Any("error", err))
				return nil, fmt.Errorf("[es] failed to initialize bulk indexer: %w", err)
			}
			esBulkIndexerCache[record.GetDestinationTableName()] = bulkIndexer
		}

		if len(req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns) == 1 {
			qValue, err := record.GetItems().GetValueByColName(
				req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns[0])
			if err != nil {
				esc.logger.Error("[es] failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("[es] failed to process record: %w", err)
			}
			docId = fmt.Sprint(qValue.Value())
		} else {
			tablePkey, err := model.RecToTablePKey(req.TableNameSchemaMapping, record)
			if err != nil {
				esc.logger.Error("[es] failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("[es] failed to process record: %w", err)
			}
			docId = base64.RawURLEncoding.EncodeToString(tablePkey.PkeyColVal[:])
		}

		err = bulkIndexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     action,
			DocumentID: docId,
			Body:       bytes.NewReader(bodyBytes),

			OnSuccess: func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem) {
				shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())
				record.PopulateCountMap(tableNameRowsMapping)
			},
			// OnFailure is called for each failed operation, log and let parent handle
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem,
				res esutil.BulkIndexerResponseItem, err error,
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
		})
		if err != nil {
			esc.logger.Error("[es] failed to add record to bulk indexer", slog.Any("error", err))
			return nil, fmt.Errorf("[es] failed to add record to bulk indexer: %w", err)
		}
		if bulkIndexFatalError != nil {
			esc.logger.Error("[es] fatal error while indexing record", slog.Any("error", bulkIndexFatalError))
			return nil, fmt.Errorf("[es] fatal error while indexing record: %w", bulkIndexFatalError)
		}
	}
	// "Receive on a closed channel yields the zero value after all elements in the channel are received."
	close(flushLoopDone)

	if cacheCloser() {
		esc.logger.Error("[es] failed to close bulk indexer(s)")
		return nil, errors.New("[es] failed to close bulk indexer(s)")
	}
	if len(bulkIndexErrors) > 0 {
		for _, err := range bulkIndexErrors {
			esc.logger.Error("[es] failed to index record", slog.Any("err", err))
		}
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := esc.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
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
