package connelasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
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
		logger:           logger.LoggerFromCtx(ctx),
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

	bulkIndexHasFatalError := false
	var bulkIndexErrors []error
	var bulkIndexMutex sync.Mutex
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
				bulkIndexMutex.Lock()
				defer bulkIndexMutex.Unlock()
				if err != nil {
					bulkIndexErrors = append(bulkIndexErrors, err)
				} else {
					causeString := ""
					if res.Error.Cause.Type != "" || res.Error.Cause.Reason != "" {
						causeString = fmt.Sprintf("(caused by type:%s reason:%s)", res.Error.Cause.Type, res.Error.Cause.Reason)
					}
					bulkIndexErrors = append(bulkIndexErrors,
						fmt.Errorf("id:%s type:%s reason:%s %s", item.DocumentID, res.Error.Type,
							res.Error.Reason, causeString))
					if res.Error.Type == "illegal_argument_exception" &&
						strings.Contains(res.Error.Reason, "Number of documents in the index can't exceed") {
						bulkIndexHasFatalError = true
					}
				}
			},
		})
		if err != nil || bulkIndexHasFatalError {
			esc.logger.Error("[es] failed to add record to bulk indexer", slog.Any("error", err))
			return 0, fmt.Errorf("[es] failed to add record to bulk indexer: %w", err)
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
	if bulkIndexHasFatalError {
		esc.logger.Error("[es] fatal error while bulk index")
		return 0, fmt.Errorf("[es] fatal error while bulk index")
	}

	err = esc.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime)
	if err != nil {
		esc.logger.Error("[es] failed to log partition info", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to log partition info: %w", err)
	}
	return numRecords, nil
}
