package connmongo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type Namespace struct {
	Db   string `bson:"db"`
	Coll string `bson:"coll"`
}

type ChangeEvent struct {
	Ns            Namespace      `bson:"ns"`
	OperationType string         `bson:"operationType"`
	DocumentKey   bson.D         `bson:"documentKey,omitempty"`
	FullDocument  bson.D         `bson:"fullDocument,omitempty"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
}

func (c *MongoConnector) GetTableSchema(
	ctx context.Context,
	_ map[string]string,
	internalVersion uint32,
	_ protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	result := make(map[string]*protos.TableSchema, len(tableMappings))
	idFieldDescription := &protos.FieldDescription{
		Name:         DefaultDocumentKeyColumnName,
		Type:         string(types.QValueKindString),
		TypeModifier: -1,
		Nullable:     false,
	}
	fullDocumentColumnName := DefaultFullDocumentColumnName
	if internalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}
	dataFieldDescription := &protos.FieldDescription{
		Name:         fullDocumentColumnName,
		Type:         string(types.QValueKindJSON),
		TypeModifier: -1,
		Nullable:     false,
	}

	for _, tm := range tableMappings {
		result[tm.SourceTableIdentifier] = &protos.TableSchema{
			TableIdentifier:       tm.SourceTableIdentifier,
			PrimaryKeyColumns:     []string{DefaultDocumentKeyColumnName},
			IsReplicaIdentityFull: true,
			System:                protos.TypeSystem_Q,
			NullableEnabled:       false,
			Columns: []*protos.FieldDescription{
				idFieldDescription,
				dataFieldDescription,
			},
		}
	}

	return result, nil
}

func (c *MongoConnector) SetupReplication(ctx context.Context, input *protos.SetupReplicationInput) (model.SetupReplicationResult, error) {
	changeStreamOpts := options.ChangeStream().
		SetComment("PeerDB changeStream").
		SetFullDocument(options.UpdateLookup)

	pipeline, err := createPipeline(nil)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to create changestream pipeline: %w", err)
	}
	changeStream, err := c.client.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to start change stream for storing initial resume token: %w", err)
	}
	defer changeStream.Close(ctx)

	c.logger.Info("SetupReplication started, waiting for initial resume token")
	var resumeToken bson.Raw
	for {
		resumeToken = changeStream.ResumeToken()
		if resumeToken != nil {
			break
		} else {
			c.logger.Info("Resume token not available, waiting for next change event...")
			if !changeStream.Next(ctx) {
				return model.SetupReplicationResult{}, fmt.Errorf("change stream error: %w", changeStream.Err())
			}
		}
	}
	err = c.SetLastOffset(ctx, input.FlowJobName, model.CdcCheckpoint{
		Text: base64.StdEncoding.EncodeToString(resumeToken),
	})
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to store initial resume token: %w", err)
	}
	c.logger.Info("SetupReplication completed, stored initial resume token")
	return model.SetupReplicationResult{}, nil
}

func (c *MongoConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	fullDocumentColumnName := DefaultFullDocumentColumnName
	if req.InternalVersion < shared.InternalVersion_MongoDBFullDocumentColumnToDoc {
		fullDocumentColumnName = LegacyFullDocumentColumnName
	}

	c.logger.Info("[mongo] started PullRecords for mirror "+req.FlowJobName,
		slog.Any("table_mapping", req.TableNameMapping),
		slog.Uint64("max_batch_size", uint64(req.MaxBatchSize)),
		slog.Duration("sync_interval", req.IdleTimeout))

	changeStreamOpts := options.ChangeStream().
		SetComment("PeerDB changeStream for mirror " + req.FlowJobName).
		SetFullDocument(options.UpdateLookup)

	var resumeToken bson.Raw
	var err error

	if req.LastOffset.Text != "" {
		// If we have a last offset, we resume from that point
		c.logger.Info("[mongo] resuming change stream", slog.String("resumeToken", req.LastOffset.Text))
		resumeToken, err = base64.StdEncoding.DecodeString(req.LastOffset.Text)
		if err != nil {
			return fmt.Errorf("failed to parse last offset: %w", err)
		}
		changeStreamOpts.SetResumeAfter(resumeToken)
	}

	pipeline, err := createPipeline(req.TableNameMapping)
	if err != nil {
		return err
	}

	changeStream, err := c.client.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		if isResumeTokenNotFoundError(err) && resumeToken != nil {
			timestamp, err := decodeTimestampFromResumeToken(resumeToken)
			if err != nil {
				return fmt.Errorf("failed to decode resume token: %w", err)
			}
			changeStreamOpts.SetStartAtOperationTime(&timestamp)
			changeStreamOpts.SetResumeAfter(nil)
			changeStream, err = c.client.Watch(ctx, pipeline, changeStreamOpts)
			if err != nil {
				return fmt.Errorf("failed to recreate change stream: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create change stream: %w", err)
		}
	}
	defer changeStream.Close(ctx)

	var recordCount uint32
	var deltaBytesProcessed, cumulativeBytesProcessed atomic.Int64
	pullStart := time.Now()
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info("[mongo] PullRecords finished streaming",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", cumulativeBytesProcessed.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()
	// before the first record arrives, we wait for up to an hour before resetting context timeout
	// after the first record arrives, we switch to configured idleTimeout
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Hour)

	reportBytesShutdown := common.Interval(ctx, time.Second*10, func() {
		read := deltaBytesProcessed.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	})

	defer func() {
		cancelTimeout()
		reportBytesShutdown()
		read := deltaBytesProcessed.Swap(0)
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, read)
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, read)
	}()

	checkpoint := func() {
		if resumeToken := changeStream.ResumeToken(); resumeToken != nil {
			resumeTokenText := base64.StdEncoding.EncodeToString(resumeToken)
			req.RecordStream.UpdateLatestCheckpointText(resumeTokenText)
		} else {
			c.logger.Warn("change stream does not currently contain a resume token")
		}
	}

	addRecordItems := func(documentKey bson.D, fullDocument bson.D, items *model.RecordItems, tableName string) error {
		if documentKey != nil {
			var idValue any
			for _, elem := range documentKey {
				if elem.Key == DefaultDocumentKeyColumnName {
					idValue = elem.Value
					break
				}
			}
			if idValue == nil {
				return exceptions.NewInvalidIdValueError(tableName)
			}
			qValue, err := qValueStringFromKey(idValue, req.InternalVersion)
			if err != nil {
				return fmt.Errorf("failed to convert _id to string: %w", err)
			}
			items.AddColumn(DefaultDocumentKeyColumnName, qValue)
		} else {
			return errors.New("document key _id not found")
		}

		if fullDocument != nil {
			qValue, err := qValueJSONFromDocument(fullDocument)
			if err != nil {
				return fmt.Errorf("failed to convert full document to JSON: %w", err)
			}
			items.AddColumn(fullDocumentColumnName, qValue)
		} else {
			// `fullDocument` field will not exist in the following scenarios:
			// 1) operationType is 'delete'
			// 2) document is deleted / collection is dropped in between update and lookup
			// 3) update changes the values for at least one of the fields in that collection's
			//    shard key (although sharding is not supported today)
			items.AddColumn(fullDocumentColumnName, types.QValueJSON{Val: "{}"})
		}
		return nil
	}

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			timeoutCtx, cancelTimeout = context.WithTimeout(ctx, req.IdleTimeout)
		}
		if recordCount%50000 == 0 {
			c.logger.Info("[mongo] PullRecords streaming",
				slog.Uint64("records", uint64(recordCount)),
				slog.Int64("bytes", cumulativeBytesProcessed.Load()),
				slog.Int("channelLen", req.RecordStream.ChannelLen()),
				slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
		}
		return nil
	}

	recreateChangeStream := func(useOperationTime bool) error {
		// extract the most recent resumeToken
		resumeToken := changeStream.ResumeToken()
		if resumeToken == nil {
			return errors.New("resume token is nil")
		}

		// close existing change stream
		if err := changeStream.Close(ctx); err != nil {
			return fmt.Errorf("failed to close change stream: %w", err)
		}

		// reset context timeout
		cancelTimeout()
		timeoutCtx, cancelTimeout = context.WithTimeout(ctx, time.Hour)

		// set resume point based on whether operation time should be used or not
		if useOperationTime {
			timestamp, err := decodeTimestampFromResumeToken(resumeToken)
			if err != nil {
				return fmt.Errorf("failed to decode resume token: %w", err)
			}
			changeStreamOpts.SetStartAtOperationTime(&timestamp)
			changeStreamOpts.SetResumeAfter(nil)
		} else {
			changeStreamOpts.SetResumeAfter(resumeToken)
			changeStreamOpts.SetStartAtOperationTime(nil)
		}

		changeStream, err = c.client.Watch(ctx, pipeline, changeStreamOpts)
		if err != nil {
			return err
		}

		return nil
	}

	for recordCount < req.MaxBatchSize {
		if ok := changeStream.Next(timeoutCtx); !ok {
			err := changeStream.Err()
			if err == nil {
				return errors.New("unexpected: changestream.Next() returned false but no change stream error was recorded")
			}

			if errors.Is(err, context.DeadlineExceeded) {
				if recordCount > 0 {
					break
				}
				// update with PostBatchResumeToken on empty batch
				// ref: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md
				checkpoint()
				// DeadlineExceeded errors are deemed not recoverable/resumable, so we have to create a new change stream instance
				if err := recreateChangeStream(false); err != nil {
					return fmt.Errorf("failed to recreate change stream: %w", err)
				}
				continue
			}

			if isResumeTokenNotFoundError(err) {
				if err := recreateChangeStream(true); err != nil {
					return fmt.Errorf("failed to recreate change stream: %w", err)
				}
				continue
			}

			return fmt.Errorf("change stream error: %w", err)
		}

		changeEventSize := int64(len(changeStream.Current))
		deltaBytesProcessed.Add(changeEventSize)
		cumulativeBytesProcessed.Add(changeEventSize)

		var changeEvent ChangeEvent
		if err := bson.Unmarshal(changeStream.Current, &changeEvent); err != nil {
			return fmt.Errorf("failed to decode change stream document: %w", err)
		}

		clusterTime := time.Unix(int64(changeEvent.ClusterTime.T), 0)
		clusterTimeNanos := clusterTime.UnixNano()
		otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, clusterTime.Unix())

		sourceTableName := fmt.Sprintf("%s.%s", changeEvent.Ns.Db, changeEvent.Ns.Coll)
		destinationTableName := req.TableNameMapping[sourceTableName].Name
		if destinationTableName == "" {
			// should never happen since pipeline should filter out irrelevant tables
			c.logger.Warn("Skipping event that cannot be mapped to a destination table %s", sourceTableName)
			continue
		}

		items := model.NewMongoRecordItems(2)
		switch changeEvent.OperationType {
		case "insert":
			if err := addRecordItems(changeEvent.DocumentKey, changeEvent.FullDocument, &items, sourceTableName); err != nil {
				return fmt.Errorf("failed to process document: %w", err)
			}

			if err = addRecord(ctx, &model.InsertRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
				Items:                items,
				SourceTableName:      sourceTableName,
				DestinationTableName: destinationTableName,
			}); err != nil {
				return fmt.Errorf("failed to add insert record: %w", err)
			}
		case "update", "replace":
			if err := addRecordItems(changeEvent.DocumentKey, changeEvent.FullDocument, &items, sourceTableName); err != nil {
				return fmt.Errorf("failed to process document: %w", err)
			}

			if err := addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
				NewItems:             items,
				SourceTableName:      sourceTableName,
				DestinationTableName: destinationTableName,
			}); err != nil {
				return fmt.Errorf("failed to add update record: %w", err)
			}
		case "delete":
			if err := addRecordItems(changeEvent.DocumentKey, changeEvent.FullDocument, &items, sourceTableName); err != nil {
				return fmt.Errorf("failed to process document: %w", err)
			}

			if err := addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
				Items:                items,
				SourceTableName:      sourceTableName,
				DestinationTableName: destinationTableName,
			}); err != nil {
				return fmt.Errorf("failed to add delete record: %w", err)
			}
		default:
			c.logger.Warn(fmt.Sprintf("skipping event with unsupported operation type '%s' (db=%s coll=%s)",
				changeEvent.OperationType, changeEvent.Ns.Db, changeEvent.Ns.Coll))
			continue
		}
		otelManager.Metrics.CommitLagGauge.Record(ctx, time.Now().UTC().Sub(clusterTime).Microseconds())
		checkpoint()
	}

	return nil
}

func createPipeline(tableNameMapping map[string]model.NameAndExclude) (mongo.Pipeline, error) {
	pipeline := mongo.Pipeline{}

	// filter out events from tables that are not in the mapping
	if tableNameMapping != nil {
		dbCollMap := make(map[string][]string)
		for dbAndTable := range tableNameMapping {
			parts := strings.SplitN(dbAndTable, ".", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("failed to create pipeline due to invalid table name: %s", dbAndTable)
			}
			db := parts[0]
			table := parts[1]
			dbCollMap[db] = append(dbCollMap[db], table)
		}

		var orCondition bson.A
		for db, tables := range dbCollMap {
			andCondition := bson.A{
				bson.D{{Key: "ns.db", Value: db}},
				bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: tables}}}},
			}
			orCondition = append(orCondition, bson.D{
				{Key: "$and", Value: andCondition},
			})
		}

		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "$or", Value: orCondition},
		}}})
	}

	// Mongo recommends using '$project' first to reduce change event size, and only use
	// '$changeStreamSplitLargeEvent' in the pipeline if still necessary. Given the document
	// themselves have a 16MB limit, project required fields for now for code simplicity.
	// ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStreamSplitLargeEvent/
	pipeline = append(pipeline,
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "operationType", Value: 1},
			{Key: "clusterTime", Value: 1},
			{Key: "documentKey", Value: 1},
			{Key: "fullDocument", Value: 1},
			{Key: "ns", Value: 1},
		}}},
	)

	return pipeline, nil
}

// This can happen if the resumeToken we are attempting to `ResumeAfter` refers to a table that has been
// filtered out of the change stream pipeline (for example, if a user pauses and edits a mirror). If
// this happens, we decode the resumeToken and extract its operation time, and start a new changeStream
// with `StartAtOperationTime` instead of `ResumeAfter`.
func isResumeTokenNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "cannot resume stream; the resume token was not found.")
}

// stubs for CDCPullConnectorCore

func (c *MongoConnector) EnsurePullability(ctx context.Context, req *protos.EnsurePullabilityBatchInput) (
	*protos.EnsurePullabilityBatchOutput, error,
) {
	return nil, nil
}

func (c *MongoConnector) ExportTxSnapshot(context.Context, string, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *MongoConnector) FinishExport(any) error {
	return nil
}

func (c *MongoConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

func (c *MongoConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MongoConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	return nil
}

func (c *MongoConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

// end stubs
