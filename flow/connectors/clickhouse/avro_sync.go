package connclickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2/ocf"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/internal/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/model"
	peerdb_clickhouse "github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type ClickHouseAvroSyncMethod struct {
	*ClickHouseConnector
	config *protos.QRepConfig
}

func NewClickHouseAvroSyncMethod(
	config *protos.QRepConfig,
	connector *ClickHouseConnector,
) *ClickHouseAvroSyncMethod {
	return &ClickHouseAvroSyncMethod{
		ClickHouseConnector: connector,
		config:              config,
	}
}

func (s *ClickHouseAvroSyncMethod) s3TableFunctionBuilder(ctx context.Context, avroFilePath string) (string, error) {
	stagingPath := s.credsProvider.BucketPath
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return "", err
	}

	endpoint := s.credsProvider.Provider.GetEndpointURL()
	region := s.credsProvider.Provider.GetRegion()
	avroFileUrl := utils.FileURLForS3Service(endpoint, region, s3o.Bucket, avroFilePath)
	creds, err := s.credsProvider.Provider.Retrieve(ctx)
	if err != nil {
		return "", err
	}
	if creds.AWS.CanExpire {
		s.logger.Info("Retrieved Temporary AWS credentials",
			slog.Time("expiryTimestamp", creds.AWS.Expires),
			slog.Duration("duration", time.Until(creds.AWS.Expires)))
	}

	var expr strings.Builder
	expr.WriteString("s3(")
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(avroFileUrl))
	expr.WriteByte(',')
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.AccessKeyID))
	expr.WriteByte(',')
	expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.SecretAccessKey))
	if creds.AWS.SessionToken != "" {
		expr.WriteByte(',')
		expr.WriteString(peerdb_clickhouse.QuoteLiteral(creds.AWS.SessionToken))
	}
	expr.WriteString(",'Avro')")
	return expr.String(), nil
}

func (s *ClickHouseAvroSyncMethod) CopyStageToDestination(ctx context.Context, avroFile utils.AvroFile) error {
	s3TableFunction, err := s.s3TableFunctionBuilder(ctx, avroFile.FilePath)
	if err != nil {
		s.logger.Error("failed to build S3 table function",
			slog.String("avroFilePath", avroFile.FilePath),
			slog.Any("error", err))
		return fmt.Errorf("failed to build S3 table function: %w", err)
	}

	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
		peerdb_clickhouse.QuoteIdentifier(s.config.DestinationTableIdentifier), s3TableFunction)
	return s.exec(ctx, query)
}

func (s *ClickHouseAvroSyncMethod) SyncRecords(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	flowJobName string,
	syncBatchID int64,
) (int64, error) {
	dstTableName := s.config.DestinationTableIdentifier

	schema, err := stream.Schema()
	if err != nil {
		return 0, err
	}
	s.logger.Info("sync function called and schema acquired",
		slog.String("dstTable", dstTableName))

	avroSchema, err := s.getAvroSchema(ctx, env, dstTableName, schema, nil)
	if err != nil {
		return 0, err
	}

	batchIdentifierForFile := fmt.Sprintf("%s_%d", shared.RandomString(16), syncBatchID)
	avroFile, err := s.writeToAvroFile(ctx, env, stream, nil, avroSchema, batchIdentifierForFile, flowJobName, nil, nil)
	if err != nil {
		return 0, err
	}

	s.logger.Info("[SyncRecords] written records to Avro file",
		slog.String("dstTable", dstTableName),
		slog.String("avroFile", avroFile.FilePath),
		slog.Int64("numRecords", avroFile.NumRecords),
		slog.Int64("syncBatchID", syncBatchID))

	if err := SetAvroStage(ctx, flowJobName, syncBatchID, avroFile); err != nil {
		return 0, fmt.Errorf("failed to set avro stage: %w", err)
	}

	return avroFile.NumRecords, nil
}

func (s *ClickHouseAvroSyncMethod) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, shared.QRepWarnings, error) {
	dstTableName := config.DestinationTableIdentifier
	startTime := time.Now()
	schema, err := stream.Schema()
	if err != nil {
		return 0, nil, err
	}

	destTypeConversions := findTypeConversions(schema, config.Columns)
	if len(destTypeConversions) > 0 {
		schema = applyTypeConversions(schema, destTypeConversions)
	}
	numericTruncator := model.NewSnapshotTableNumericTruncator(dstTableName, schema.Fields)

	columnNameAvroFieldMap := model.ConstructColumnNameAvroFieldMap(schema.Fields)
	avroFiles, totalRecords, err := s.pushDataToS3ForSnapshot(ctx, config, dstTableName, schema,
		columnNameAvroFieldMap, partition, stream, destTypeConversions, numericTruncator)
	if err != nil {
		s.logger.Error("failed to push data to S3",
			slog.String("dstTable", dstTableName),
			slog.Any("error", err))
		return 0, nil, err
	}

	if err := s.pushS3DataToClickHouseForSnapshot(
		ctx, avroFiles, schema, columnNameAvroFieldMap, config); err != nil {
		s.logger.Error("failed to push data to ClickHouse",
			slog.String("dstTable", dstTableName),
			slog.Any("error", err))
		return 0, nil, err
	}
	warnings := numericTruncator.Warnings()

	if err := s.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		s.logger.Error("Failed to finish QRep partition", slog.Any("error", err))
		return 0, nil, err
	}

	return totalRecords, warnings, nil
}

func (s *ClickHouseAvroSyncMethod) pushDataToS3ForSnapshot(
	ctx context.Context,
	config *protos.QRepConfig,
	dstTableName string,
	schema types.QRecordSchema,
	columnNameAvroFieldMap map[string]string,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
	destTypeConversions map[string]types.TypeConversion,
	numericTruncator model.SnapshotTableNumericTruncator,
) ([]utils.AvroFile, int64, error) {
	avroSchema, err := s.getAvroSchema(ctx, config.Env, dstTableName, schema, columnNameAvroFieldMap)
	if err != nil {
		return nil, 0, err
	}

	bytesPerAvroFile, err := internal.PeerDBS3BytesPerAvroFile(ctx, config.Env)
	if err != nil {
		return nil, 0, err
	}

	// track uncompressed bytes for MongoDB due to high JSON compression ratio
	trackUncompressed := config.SourceType == protos.DBType_MONGO

	s.logger.Info("writing avro chunks to S3 start",
		slog.String("partitionId", partition.PartitionId),
		slog.Int64("bytesPerAvroFile", bytesPerAvroFile))

	// helper function to create a substream for splitting the main stream into chunks
	createChunkedSubstream := func(done *atomic.Bool) (*model.QRecordStream, *model.QRecordAvroChunkSizeTracker) {
		substream := model.NewQRecordStream(0)
		substream.SetSchema(schema)
		substream.SetSchemaDebug(stream.SchemaDebug())
		sizeTracker := model.QRecordAvroChunkSizeTracker{TrackUncompressed: trackUncompressed}
		go func() {
			recordsDone := true
			for record := range stream.Records {
				substream.Records <- record
				if sizeTracker.Bytes.Load() >= bytesPerAvroFile {
					recordsDone = false
					break
				}
			}
			if recordsDone {
				done.Store(true)
			}
			substream.Close(stream.Err())
		}()
		return substream, &sizeTracker
	}

	var avroFiles []utils.AvroFile
	var totalRecords int64

	if bytesPerAvroFile != 0 {
		chunkNum := 0
		var done atomic.Bool
		for !done.Load() {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}

			substream, sizeTracker := createChunkedSubstream(&done)
			subFile, err := s.writeToAvroFile(ctx, config.Env, substream, sizeTracker, avroSchema,
				fmt.Sprintf("%s.%06d", partition.PartitionId, chunkNum),
				config.FlowJobName, destTypeConversions, numericTruncator,
			)
			if err != nil {
				return nil, 0, err
			}
			avroFiles = append(avroFiles, subFile)
			chunkNum += 1
			totalRecords += subFile.NumRecords
		}

		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	} else {
		avroFile, err := s.writeToAvroFile(
			ctx, config.Env, stream, nil, avroSchema, partition.PartitionId, config.FlowJobName,
			destTypeConversions, numericTruncator,
		)
		if err != nil {
			return nil, 0, err
		}
		avroFiles = append(avroFiles, avroFile)
		totalRecords = avroFile.NumRecords
	}

	s.logger.Info("finished writing avro chunks to S3",
		slog.String("partitionId", partition.PartitionId),
		slog.Int("totalChunks", len(avroFiles)),
		slog.Int64("totalRecords", totalRecords))

	return avroFiles, totalRecords, nil
}

func (s *ClickHouseAvroSyncMethod) pushS3DataToClickHouseForSnapshot(
	ctx context.Context,
	avroFiles []utils.AvroFile,
	schema types.QRecordSchema,
	columnNameAvroFieldMap map[string]string,
	config *protos.QRepConfig,
) error {
	insertConfig := &insertFromTableFunctionConfig{
		destinationTable: config.DestinationTableIdentifier,
		schema:           schema,
		columnNameMap:    columnNameAvroFieldMap,
		excludedColumns:  config.Exclude,
		config:           config,
		connector:        s.ClickHouseConnector,
		logger:           s.logger,
	}

	numParts, err := internal.PeerDBClickHouseInitialLoadPartsPerPartition(ctx, s.config.Env)
	if err != nil {
		s.logger.Warn("failed to get chunking parts, proceeding without chunking", slog.Any("error", err))
		numParts = 1
	}
	numParts = max(numParts, 1)

	chSettings := clickhouse.NewCHSettings(s.chVersion)
	chSettings.Add(clickhouse.SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	chSettings.Add(clickhouse.SettingTypeJsonSkipDuplicatedPaths, "1")
	if config.Version >= shared.InternalVersion_JsonEscapeDotsInKeys {
		chSettings.Add(clickhouse.SettingJsonTypeEscapeDotsInKeys, "1")
	}

	// Process each chunk file individually
	for chunkIdx, avroFile := range avroFiles {
		s.logger.Info("processing chunk",
			slog.Int("chunkIdx", chunkIdx),
			slog.Int("totalChunks", len(avroFiles)),
			slog.String("avroFilePath", avroFile.FilePath))

		for i := range numParts {
			// Get fresh credentials for each part
			s3TableFunction, err := s.s3TableFunctionBuilder(ctx, avroFile.FilePath)
			if err != nil {
				s.logger.Error("failed to build S3 table function",
					slog.String("avroFilePath", avroFile.FilePath),
					slog.Any("error", err),
					slog.Uint64("part", i),
					slog.Uint64("numParts", numParts),
					slog.Int("chunkIdx", chunkIdx),
				)
				return fmt.Errorf("failed to build S3 table function: %w", err)
			}

			var query string
			if numParts > 1 {
				query, err = buildInsertFromTableFunctionQueryWithPartitioning(
					ctx, insertConfig, s3TableFunction, i, numParts, chSettings)
			} else {
				query, err = buildInsertFromTableFunctionQuery(ctx, insertConfig, s3TableFunction, chSettings)
			}
			if err != nil {
				s.logger.Error("failed to build insert query",
					slog.String("avroFilePath", avroFile.FilePath),
					slog.Any("error", err),
					slog.Uint64("part", i),
					slog.Uint64("numParts", numParts),
					slog.Int("chunkIdx", chunkIdx),
				)
				return fmt.Errorf("failed to build insert query: %w", err)
			}

			s.logger.Info("inserting part",
				slog.Uint64("part", i),
				slog.Uint64("numParts", numParts),
				slog.Int("chunkIdx", chunkIdx),
				slog.Int("totalChunks", len(avroFiles)))

			if err := s.exec(ctx, query); err != nil {
				s.logger.Error("failed to insert part",
					slog.Uint64("part", i),
					slog.Uint64("numParts", numParts),
					slog.Int("chunkIdx", chunkIdx),
					slog.Any("error", err))
				return exceptions.NewQRepSyncError(err, config.DestinationTableIdentifier, s.ClickHouseConnector.Config.Database)
			}

			s.logger.Info("inserted part",
				slog.Uint64("part", i),
				slog.Uint64("numParts", numParts),
				slog.Int("chunkIdx", chunkIdx),
				slog.Int("totalChunks", len(avroFiles)))
		}

		s.logger.Info("processed chunk",
			slog.Int("chunkIdx", chunkIdx),
			slog.Int("totalChunks", len(avroFiles)),
			slog.String("avroFilePath", avroFile.FilePath))
	}

	return nil
}

func (s *ClickHouseAvroSyncMethod) getAvroSchema(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	schema types.QRecordSchema,
	avroNameMap map[string]string,
) (*model.QRecordAvroSchemaDefinition, error) {
	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, dstTableName, schema, protos.DBType_CLICKHOUSE, avroNameMap)
	if err != nil {
		return nil, fmt.Errorf("failed to define Avro schema: %w", err)
	}
	return avroSchema, nil
}

func (s *ClickHouseAvroSyncMethod) writeToAvroFile(
	ctx context.Context,
	env map[string]string,
	stream *model.QRecordStream,
	sizeTracker *model.QRecordAvroChunkSizeTracker,
	avroSchema *model.QRecordAvroSchemaDefinition,
	identifierForFile string,
	flowJobName string,
	typeConversions map[string]types.TypeConversion,
	numericTruncator model.SnapshotTableNumericTruncator,
) (utils.AvroFile, error) {
	stagingPath := s.credsProvider.BucketPath
	ocfWriter := utils.NewPeerDBOCFWriter(stream, avroSchema, ocf.ZStandard, protos.DBType_CLICKHOUSE, sizeTracker)
	s3o, err := utils.NewS3BucketAndPrefix(stagingPath)
	if err != nil {
		return utils.AvroFile{}, fmt.Errorf("failed to parse staging path: %w", err)
	}

	s3UuidPrefix, err := internal.PeerDBS3UuidPrefix(ctx, s.config.Env)
	if err != nil {
		return utils.AvroFile{}, err
	}

	var s3AvroFileKey string
	if s3UuidPrefix {
		s3AvroFileKey = fmt.Sprintf("%s/%s/%s/%s.avro", s3o.Prefix, uuid.NewString(), flowJobName, identifierForFile)
	} else {
		s3AvroFileKey = fmt.Sprintf("%s/%s/%s.avro", s3o.Prefix, flowJobName, identifierForFile)
	}
	s3AvroFileKey = strings.TrimLeft(s3AvroFileKey, "/")
	avroFile, err := ocfWriter.WriteRecordsToS3(
		ctx, env, s3o.Bucket, s3AvroFileKey, s.credsProvider.Provider, typeConversions, numericTruncator,
	)
	if err != nil {
		return utils.AvroFile{}, fmt.Errorf("failed to write records to S3: %w", err)
	}

	return avroFile, nil
}

func (s *ClickHouseAvroSyncMethod) SyncQRepObjects(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QObjectStream,
) (int64, shared.QRepWarnings, error) {
	// Delegate to the ClickHouse connector's implementation
	return s.ClickHouseConnector.SyncQRepObjects(ctx, config, partition, stream)
}
