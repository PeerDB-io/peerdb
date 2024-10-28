package connelasticsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esutil"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (esc *ElasticsearchConnector) SetupQRepMetadataTables(ctx context.Context,
	config *protos.QRepConfig,
) error {
	return nil
}

func upsertKeyColsHash(qRecord []qvalue.QValue, upsertColIndices []int) string {
	hasher := sha256.New()

	for _, upsertColIndex := range upsertColIndices {
		// cannot return an error
		_, _ = fmt.Fprint(hasher, qRecord[upsertColIndex].Value())
	}
	hashBytes := hasher.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(hashBytes)
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

	// len == 0 means use UUID
	// len == 1 means single column, use value directly
	// len > 1 means SHA256 hash of upsert key columns
	// ordered such that we preserve order of UpsertKeyColumns
	var upsertKeyColIndices []int
	if config.WriteMode.WriteType == protos.QRepWriteType_QREP_WRITE_MODE_UPSERT {
		schemaColNames := schema.GetColumnNames()
		for _, upsertCol := range config.WriteMode.UpsertKeyColumns {
			idx := slices.Index(schemaColNames, upsertCol)
			if idx != -1 {
				upsertKeyColIndices = append(upsertKeyColIndices, idx)
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

		switch len(upsertKeyColIndices) {
		case 0:
			// relying on autogeneration of document ID
		case 1:
			docId = fmt.Sprint(qRecord[upsertKeyColIndices[0]].Value())
		default:
			docId = upsertKeyColsHash(qRecord, upsertKeyColIndices)
		}
		for i, field := range schema.Fields {
			if r, ok := qRecord[i].(qvalue.QValueJSON); ok { // JSON is stored as a string, fix that
				qRecordJsonMap[field.Name] = json.RawMessage(
					shared.UnsafeFastStringToReadOnlyBytes(r.Val))
			} else {
				qRecordJsonMap[field.Name] = qRecord[i].Value()
			}
		}
		qRecordJsonBytes, err := json.Marshal(qRecordJsonMap)
		if err != nil {
			esc.logger.Error("[es] failed to json.Marshal record", slog.Any("error", err))
			return 0, fmt.Errorf("[es] failed to json.Marshal record: %w", err)
		}

		err = esBulkIndexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     actionIndex,
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
