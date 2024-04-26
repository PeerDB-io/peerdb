package connelasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
)

func (esc *ElasticsearchConnector) SetupQRepMetadataTables(ctx context.Context,
	config *protos.QRepConfig,
) error {
	return nil
}

func (esc *ElasticsearchConnector) SyncQRepRecords(ctx context.Context, config *protos.QRepConfig,
	partition *protos.QRepPartition, stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()

	schema := stream.Schema()

	var bulkIndexFatalError error
	var bulkIndexErrors []error
	var bulkIndexOnFailureMutex sync.Mutex
	var docId string
	numRecords := 0
	bulkIndexerHasShutdown := false

	// -1 means use UUID, >=0 means column in the record
	upsertColIndex := -1
	// only support single upsert column for now
	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT &&
		len(config.WriteMode.UpsertKeyColumns) == 1 {
		for i, field := range schema.Fields {
			if config.WriteMode.UpsertKeyColumns[0] == field.Name {
				upsertColIndex = i
			}
		}
	}

	esBulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  config.DestinationTableIdentifier,
		Client: esc.client,
		// parallelism comes from the workflow design itself, no need for this
		NumWorkers:    1,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		esc.logger.Error("[es] failed to initialize bulk indexer", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to initialize bulk indexer: %w", err)
	}
	defer func() {
		if !bulkIndexerHasShutdown {
			err := esBulkIndexer.Close(context.Background())
			if err != nil {
				esc.logger.Error("[es] failed to close bulk indexer", slog.Any("error", err))
			}
		}
	}()

	for qRecord := range stream.Records {
		qRecordJsonMap := make(map[string]any)

		if upsertColIndex >= 0 {
			docId = fmt.Sprintf("%v", qRecord[upsertColIndex].Value())
		} else {
			docId = uuid.New().String()
		}
		for i, field := range schema.Fields {
			switch r := qRecord[i].(type) {
			// JSON is stored as a string, fix that
			case qvalue.QValueJSON:
				qRecordJsonMap[field.Name] = json.RawMessage(shared.
					UnsafeFastStringToReadOnlyBytes(r.Val))
			default:
				qRecordJsonMap[field.Name] = r.Value()
			}
		}
		qRecordJsonBytes, err := json.Marshal(qRecordJsonMap)
		if err != nil {
			esc.logger.Error("[es] failed to json.Marshal record", slog.Any("error", err))
			return 0, fmt.Errorf("[es] failed to json.Marshal record: %w", err)
		}

		err = esBulkIndexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: docId,
			Body:       bytes.NewReader(qRecordJsonBytes),

			// OnFailure is called for each failed operation, log and let parent handle
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem,
				res esutil.BulkIndexerResponseItem, err error,
			) {
				bulkIndexOnFailureMutex.Lock()
				defer bulkIndexOnFailureMutex.Unlock()
				if err != nil {
					bulkIndexErrors = append(bulkIndexErrors, err)
				} else {
					causeString := ""
					if res.Error.Cause.Type != "" || res.Error.Cause.Reason != "" {
						causeString = fmt.Sprintf("(caused by type:%s reason:%s)", res.Error.Cause.Type, res.Error.Cause.Reason)
					}
					cbErr := fmt.Errorf("id:%s type:%s reason:%s %s", item.DocumentID, res.Error.Type,
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
			return 0, fmt.Errorf("[es] failed to add record to bulk indexer: %w", err)
		}
		if bulkIndexFatalError != nil {
			esc.logger.Error("[es] fatal error while indexing record", slog.Any("error", bulkIndexFatalError))
			return 0, fmt.Errorf("[es] fatal error while indexing record: %w", bulkIndexFatalError)
		}

		// update here instead of OnSuccess, if we close successfully it should match
		numRecords++
	}

	if err := stream.Err(); err != nil {
		esc.logger.Error("[es] failed to get record from stream", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to get record from stream: %w", err)
	}
	if err := esBulkIndexer.Close(ctx); err != nil {
		esc.logger.Error("[es] failed to close bulk indexer", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to close bulk indexer: %w", err)
	}
	bulkIndexerHasShutdown = true
	if len(bulkIndexErrors) > 0 {
		for _, err := range bulkIndexErrors {
			esc.logger.Error("[es] failed to index record", slog.Any("err", err))
		}
	}

	err = esc.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime)
	if err != nil {
		esc.logger.Error("[es] failed to log partition info", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to log partition info: %w", err)
	}
	return numRecords, nil
}
