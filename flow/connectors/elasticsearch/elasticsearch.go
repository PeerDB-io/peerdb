package conn_elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
)

type ElasticsearchConnector struct {
	pgMetadata *metadataStore.PostgresMetadata
	client     *elasticsearch.Client
	logger     log.Logger
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
		client:     esClient,
		pgMetadata: pgMetadata,
		logger:     logger.LoggerFromCtx(ctx),
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

func (esc *ElasticsearchConnector) IsQRepPartitionSynced(ctx context.Context,
	req *protos.IsQRepPartitionSyncedInput,
) (bool, error) {
	return esc.pgMetadata.IsQRepPartitionSynced(ctx, req)
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

	var bulkIndexHasError bool
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
		// 1GB
		FlushBytes: 1_000_000_000,
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

	for qRecordOrErr := range stream.Records {
		if qRecordOrErr.Err != nil {
			esc.logger.Error("[es] failed to get record from stream", slog.Any("error", qRecordOrErr.Err))
			return 0, fmt.Errorf("[es] failed to get record from stream: %w", qRecordOrErr.Err)
		}

		qRecordJsonMap := make(map[string]any)

		if upsertColIndex >= 0 {
			docId = fmt.Sprintf("%v", qRecordOrErr.Record[upsertColIndex].Value())
		} else {
			docId = uuid.New().String()
		}
		for i, field := range schema.Fields {
			switch field.Type {
			// JSON is stored as a string, fix that
			case qvalue.QValueKindJSON:
				var jsonUnnest any
				err := json.Unmarshal([]byte(qRecordOrErr.Record[i].Value().(string)), &jsonUnnest)
				if err != nil {
					esc.logger.Error("[es] failed to json.Unmarshal subfield", slog.Any("error", qRecordOrErr.Err))
					return 0, fmt.Errorf("[es] failed to json.Unmarshal subfield: %w", qRecordOrErr.Err)
				}
				qRecordJsonMap[field.Name] = jsonUnnest
			default:
				qRecordJsonMap[field.Name] = qRecordOrErr.Record[i].Value()
			}
		}
		qRecordJsonBytes, err := json.Marshal(qRecordJsonMap)
		if err != nil {
			esc.logger.Error("[es] failed to json.Marshal record", slog.Any("error", err))
			return 0, fmt.Errorf("[es] failed to json.Marshal record: %w", err)
		}

		esBulkIndexer.Add(ctx, esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: docId,
			Body:       bytes.NewReader(qRecordJsonBytes),

			// OnFailure is called for each failed operation, log and let parent handle
			OnFailure: func(ctx context.Context, _ esutil.BulkIndexerItem,
				_ esutil.BulkIndexerResponseItem, err error,
			) {
				bulkIndexMutex.Lock()
				defer bulkIndexMutex.Unlock()
				bulkIndexHasError = true
				bulkIndexErrors = append(bulkIndexErrors, err)
			},
		})

		// update here instead of OnSuccess, if we close successfully it should match
		numRecords++
	}

	if err := esBulkIndexer.Close(ctx); err != nil {
		esc.logger.Error("[es] failed to close bulk indexer", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to close bulk indexer: %w", err)
	}
	bulkIndexerHasShutdown = true
	if bulkIndexHasError {
		esc.logger.Error("[es] failed to bulk index records", slog.Any("errors", bulkIndexErrors))
		return 0, fmt.Errorf("[es] failed to bulk index records: %v", bulkIndexErrors)
	}

	err = esc.pgMetadata.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime)
	if err != nil {
		esc.logger.Error("[es] failed to log partition info", slog.Any("error", err))
		return 0, fmt.Errorf("[es] failed to log partition info: %w", err)
	}
	return numRecords, nil
}
