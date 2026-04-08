package connelasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

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

type tableUpsertCol struct {
	table string
	col   string
}

type ElasticsearchConnector struct {
	*metadataStore.PostgresMetadata
	client                   searchClient
	backend                  searchBackend
	logger                   log.Logger
	hushWarnUpsertColMissing map[tableUpsertCol]struct{}
}

func NewElasticsearchConnector(ctx context.Context,
	config *protos.ElasticsearchConfig,
) (*ElasticsearchConnector, error) {
	client, err := newSearchClient(ctx, config)
	if err != nil {
		return nil, err
	}
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &ElasticsearchConnector{
		PostgresMetadata:         pgMetadata,
		client:                   client,
		backend:                  client.Backend(),
		logger:                   internal.LoggerFromCtx(ctx),
		hushWarnUpsertColMissing: make(map[tableUpsertCol]struct{}),
	}, nil
}

func (esc *ElasticsearchConnector) ConnectionActive(ctx context.Context) error {
	if err := esc.client.DiscoverNodes(); err != nil {
		return fmt.Errorf("failed to check if %s peer is active: %w", esc.backend, err)
	}
	return nil
}

func (esc *ElasticsearchConnector) Close() error {
	// stateless connector
	return nil
}

func (esc *ElasticsearchConnector) logPrefix() string {
	return esc.backend.logPrefix()
}

// ES is queue-like, no raw table staging needed
func (esc *ElasticsearchConnector) CreateRawTable(ctx context.Context,
	req *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

// we handle schema changes by not handling them since no mapping is being enforced right now
func (esc *ElasticsearchConnector) ReplayTableSchemaDeltas(ctx context.Context, env map[string]string,
	flowJobName string, _ []*protos.TableMapping, schemaDeltas []*protos.TableSchemaDelta, _ []string,
) error {
	return nil
}

func recordItemsProcessor(items model.RecordItems) ([]byte, error) {
	qRecordJSONMap := make(map[string]any)

	for key, val := range items.ColToVal {
		if r, ok := val.(types.QValueJSON); ok { // JSON is stored as a string, fix that
			qRecordJSONMap[key] = json.RawMessage(
				shared.UnsafeFastStringToReadOnlyBytes(r.Val))
		} else {
			qRecordJSONMap[key] = val.Value()
		}
	}

	return json.Marshal(qRecordJSONMap)
}

func (esc *ElasticsearchConnector) SyncRecords(ctx context.Context,
	req *model.SyncRecordsRequest[model.RecordItems],
) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	var lastSeenLSN atomic.Int64
	var numRecords int64

	bulkIndexerCache := make(map[string]searchBulkIndexer)
	bulkIndexersHaveShutdown := false
	cacheCloser := func() bool {
		closeHasErrors := false
		if !bulkIndexersHaveShutdown {
			for bulkIndexer := range maps.Values(bulkIndexerCache) {
				if err := bulkIndexer.Close(context.Background()); err != nil {
					esc.logger.Error(esc.logPrefix()+" failed to close bulk indexer", slog.Any("error", err))
					closeHasErrors = true
				}
				numRecords += int64(bulkIndexer.NumFlushed())
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
			esc.logger.Warn(esc.logPrefix()+" failed to get flush timeout, no periodic flushing", slog.Any("error", err))
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
					if err := esc.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{ID: lastSeen}); err != nil {
						esc.logger.Warn(esc.logPrefix()+" SetLastOffset error", slog.Any("error", err))
					} else {
						shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
						esc.logger.Info(esc.logPrefix()+" updated last offset",
							slog.Int64("lastOffset", lastSeen))
					}
				}
			}
		}
	}()

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
				esc.logger.Error(esc.logPrefix()+" failed to json.Marshal record", slog.Any("error", err))
				return nil, fmt.Errorf("%s failed to json.Marshal record: %w", esc.logPrefix(), err)
			}
		case *model.DeleteRecord[model.RecordItems]:
			action = actionDelete
			bodyBytes = nil
		}

		bulkIndexer, ok := bulkIndexerCache[record.GetDestinationTableName()]
		if !ok {
			bulkIndexer, err = esc.client.NewBulkIndexer(record.GetDestinationTableName())
			if err != nil {
				esc.logger.Error(esc.logPrefix()+" failed to initialize bulk indexer", slog.Any("error", err))
				return nil, fmt.Errorf("%s failed to initialize bulk indexer: %w", esc.logPrefix(), err)
			}
			bulkIndexerCache[record.GetDestinationTableName()] = bulkIndexer
		}

		var docID string
		if len(req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns) == 1 {
			qValue, err := record.GetItems().GetValueByColName(
				req.TableNameSchemaMapping[record.GetDestinationTableName()].PrimaryKeyColumns[0])
			if err != nil {
				esc.logger.Error(esc.logPrefix()+" failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("%s failed to process record: %w", esc.logPrefix(), err)
			}
			docID = fmt.Sprint(qValue.Value())
		} else {
			tablePKey, err := model.RecToTablePKey(req.TableNameSchemaMapping, record)
			if err != nil {
				esc.logger.Error(esc.logPrefix()+" failed to process record", slog.Any("error", err))
				return nil, fmt.Errorf("%s failed to process record: %w", esc.logPrefix(), err)
			}
			docID = base64.RawURLEncoding.EncodeToString(tablePKey.PkeyColVal[:])
		}

		if err := bulkIndexer.Add(ctx, searchBulkIndexerItem{
			Action:     action,
			DocumentID: docID,
			Body:       bytes.NewReader(bodyBytes),
			OnSuccess: func() {
				shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())
				record.PopulateCountMap(tableNameRowsMapping)
			},
			OnFailure: func(failure searchBulkIndexerFailure) {
				if action == actionDelete && failure.Status == 404 {
					return
				}
				bulkIndexOnFailureMutex.Lock()
				defer bulkIndexOnFailureMutex.Unlock()
				cbErr := searchFailureToError(docID, failure)
				bulkIndexErrors = append(bulkIndexErrors, cbErr)
				if failure.ErrorType == "illegal_argument_exception" {
					bulkIndexFatalError = cbErr
				}
			},
		}); err != nil {
			esc.logger.Error(esc.logPrefix()+" failed to add record to bulk indexer", slog.Any("error", err))
			return nil, fmt.Errorf("%s failed to add record to bulk indexer: %w", esc.logPrefix(), err)
		}
		if bulkIndexFatalError != nil {
			esc.logger.Error(esc.logPrefix()+" fatal error while indexing record", slog.Any("error", bulkIndexFatalError))
			return nil, fmt.Errorf("%s fatal error while indexing record: %w", esc.logPrefix(), bulkIndexFatalError)
		}
	}
	close(flushLoopDone)

	if cacheCloser() {
		esc.logger.Error(esc.logPrefix() + " failed to close bulk indexer(s)")
		return nil, fmt.Errorf("%s failed to close bulk indexer(s)", esc.logPrefix())
	}
	if len(bulkIndexErrors) > 0 {
		for _, err := range bulkIndexErrors {
			esc.logger.Error(esc.logPrefix()+" failed to index record", slog.Any("err", err))
		}
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	if err := esc.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
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
