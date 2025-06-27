package connmongo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	DefaultDocumentKeyColumnName  = "_id"
	DefaultFullDocumentColumnName = "_full_document"
)

type MongoConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MongoConfig
	client *mongo.Client
	logger log.Logger
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(options.Client().
		SetAppName("PeerDB Mongo Connector").
		SetReadPreference(readpref.Primary()).
		SetCompressors([]string{"zstd", "snappy"}).
		ApplyURI(config.Uri))
	if err != nil {
		return nil, err
	}
	return &MongoConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		client:           client,
		logger:           logger,
	}, nil
}

func (c *MongoConnector) Close() error {
	if c != nil && c.client != nil {
		// Use a timeout to ensure the disconnect operation does not hang indefinitely
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return c.client.Disconnect(timeout)
	}
	return nil
}

func (c *MongoConnector) ConnectionActive(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	return nil
}

func (c *MongoConnector) GetVersion(ctx context.Context) (string, error) {
	db := c.client.Database("admin")

	var buildInfoDoc bson.M
	if err := db.RunCommand(ctx, bson.D{bson.E{Key: "buildInfo", Value: 1}}).Decode(&buildInfoDoc); err != nil {
		return "", fmt.Errorf("failed to run buildInfo command: %w", err)
	}

	version, ok := buildInfoDoc["version"].(string)
	if !ok {
		return "", fmt.Errorf("buildInfo.version is not a string, but %T", buildInfoDoc["version"])
	}
	return version, nil
}

func (c *MongoConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigs) error {
	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	// Check if MongoDB is configured as a replica set
	var result bson.M
	if err := c.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetGetStatus", Value: 1},
	}).Decode(&result); err != nil {
		return fmt.Errorf("failed to get replica set status: %w", err)
	}

	// Check if this is a replica set
	if _, ok := result["set"]; !ok {
		return errors.New("MongoDB is not configured as a replica set, which is required for CDC")
	}

	if myState, ok := result["myState"]; !ok {
		return errors.New("myState not found in response")
	} else if myStateInt, ok := myState.(int32); !ok {
		return fmt.Errorf("failed to convert myState %v to int32", myState)
	} else if myStateInt != 1 {
		return fmt.Errorf("MongoDB is not the primary node in the replica set, current state: %d", myState)
	}

	return nil
}

func (c *MongoConnector) GetTableSchema(
	ctx context.Context,
	_ map[string]string,
	_ uint32,
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
	dataFieldDescription := &protos.FieldDescription{
		Name:         DefaultFullDocumentColumnName,
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
		SetFullDocument(options.UpdateLookup).
		SetFullDocumentBeforeChange(options.WhenAvailable)
	changeStream, err := c.client.Watch(ctx, mongo.Pipeline{}, changeStreamOpts)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to start change stream for storing initial resume token: %w", err)
	}
	defer changeStream.Close(ctx)

	c.logger.Info("SetupReplication started, waiting for initial resume token",
		slog.String("flowJobName", input.FlowJobName))
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
	c.logger.Info("SetupReplication completed, stored initial resume token",
		slog.String("flowJobName", input.FlowJobName))
	return model.SetupReplicationResult{}, nil
}

// stubs for CDCPullConnectorCore

func (c *MongoConnector) EnsurePullability(ctx context.Context, req *protos.EnsurePullabilityBatchInput) (
	*protos.EnsurePullabilityBatchOutput, error,
) {
	return nil, nil
}

func (c *MongoConnector) ExportTxSnapshot(context.Context, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *MongoConnector) FinishExport(any) error {
	return nil
}

func (c *MongoConnector) SetupReplConn(context.Context) error {
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

func (c *MongoConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool shared.CatalogPool,
	alertKeys *alerting.AlertKeys,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	return nil
}

func (c *MongoConnector) GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error) {
	return nil, nil
}

func (c *MongoConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	return nil
}

func (c *MongoConnector) RemoveTablesFromPublication(ctx context.Context, req *protos.RemoveTablesFromPublicationInput) error {
	return nil
}

// end stubs

func (c *MongoConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()
	c.logger.Info("[started] PullRecords for mirror "+req.FlowJobName,
		slog.Any("table_mapping", req.TableNameMapping),
		slog.Uint64("max_batch_size", uint64(req.MaxBatchSize)),
		slog.Duration("idle_timeout", req.IdleTimeout))

	changeStreamOpts := options.ChangeStream().
		SetComment("PeerDB changeStream for mirror " + req.FlowJobName).
		SetFullDocument(options.UpdateLookup).
		SetFullDocumentBeforeChange(options.WhenAvailable)
	if req.LastOffset.Text != "" {
		// If we have a last offset, we resume from that point
		c.logger.Info("[mongo] resuming change stream", slog.String("resumeToken", req.LastOffset.Text))
		resumeTokenBytes, err := base64.StdEncoding.DecodeString(req.LastOffset.Text)
		if err != nil {
			return fmt.Errorf("failed to parse last offset: %w", err)
		}
		changeStreamOpts.SetResumeAfter(bson.Raw(resumeTokenBytes))
	}

	changeStream, err := c.client.Watch(ctx, mongo.Pipeline{}, changeStreamOpts)
	if err != nil {
		var cmdErr mongo.CommandError
		// ChangeStreamHistoryLost is basically slot invalidation
		if errors.As(err, &cmdErr) && cmdErr.Code == 286 {
			return errors.New("change stream history lost")
		}
		return err
	}
	defer changeStream.Close(ctx)
	c.logger.Info("ChangeStream started for mirror " + req.FlowJobName)

	var recordCount uint32
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", recordCount))
	}()
	// before first record, we wait indefinitely so give it ctx
	// after first record, we wait for idle timeout
	getCtx := ctx
	var cancelTimeout context.CancelFunc
	defer func() {
		if cancelTimeout != nil {
			cancelTimeout()
		}
	}()
	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			// after the first record, we switch to a timeout context
			getCtx, cancelTimeout = context.WithTimeout(ctx, req.IdleTimeout)
		}
		return nil
	}

	for recordCount < req.MaxBatchSize && changeStream.Next(getCtx) {
		var changeDoc bson.M
		if err := changeStream.Decode(&changeDoc); err != nil {
			return fmt.Errorf("failed to decode change stream document: %w", err)
		}

		if operationType, ok := changeDoc["operationType"]; !ok {
			c.logger.Warn("operationType field not found")
			continue
		} else if operationType != "insert" && operationType != "update" && operationType != "replace" && operationType != "delete" {
			continue
		}

		clusterTime := changeDoc["clusterTime"].(bson.Timestamp)
		clusterTimeNanos := time.Unix(int64(clusterTime.T), 0).UnixNano()

		sourceTableName := fmt.Sprintf("%s.%s", changeDoc["ns"].(bson.D)[0].Value, changeDoc["ns"].(bson.D)[1].Value)
		destinationTableName := req.TableNameMapping[sourceTableName].Name

		items := model.NewRecordItems(2, true)

		if documentKey, found := changeDoc["documentKey"]; found {
			if len(documentKey.(bson.D)) == 0 || documentKey.(bson.D)[0].Key != DefaultDocumentKeyColumnName {
				// should never happen
				return errors.New("invalid document key, expect _id")
			}
			id := documentKey.(bson.D)[0].Value
			qValue, err := qValueStringFromKey(id)
			if err != nil {
				return fmt.Errorf("failed to convert _id to string: %w", err)
			}
			items.AddColumn(DefaultDocumentKeyColumnName, qValue)
		} else {
			// should never happen
			return errors.New("documentKey field not found")
		}

		if fullDocument, found := changeDoc["fullDocument"]; found {
			qValue, err := qValueJSONFromDocument(fullDocument.(bson.D))
			if err != nil {
				return fmt.Errorf("failed to convert fullDocument to JSON: %w", err)
			}
			items.AddColumn(DefaultFullDocumentColumnName, qValue)
		} else {
			// `fullDocument` field will not exist in the following scenarios:
			// 1) operationType is 'delete'
			// 2) document is deleted / collection is dropped in between update and lookup
			// 3) update changes the values for at least one of the fields in that collection's
			//    shard key (although sharding is not supported today)
			items.AddColumn(DefaultFullDocumentColumnName, types.QValueJSON{Val: "{}"})
		}

		if operationType, ok := changeDoc["operationType"]; ok {
			switch operationType {
			case "insert":
				if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
					BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
					Items:                items,
					SourceTableName:      sourceTableName,
					DestinationTableName: destinationTableName,
				}); err != nil {
					return fmt.Errorf("failed to add insert record: %w", err)
				}
			case "update", "replace":
				if err := addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
					BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
					NewItems:             items,
					SourceTableName:      sourceTableName,
					DestinationTableName: destinationTableName,
				}); err != nil {
					return fmt.Errorf("failed to add update record: %w", err)
				}
			case "delete":
				if err := addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
					BaseRecord:           model.BaseRecord{CommitTimeNano: clusterTimeNanos},
					Items:                items,
					SourceTableName:      sourceTableName,
					DestinationTableName: destinationTableName,
				}); err != nil {
					return fmt.Errorf("failed to add delete record: %w", err)
				}
			default:
				return fmt.Errorf("unsupported operationType: %s", operationType)
			}
		}
	}
	if err := changeStream.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		c.logger.Error("PullRecords change stream error", "error", err)
		return fmt.Errorf("change stream error: %w", err)
	}
	if resumeToken := changeStream.ResumeToken(); resumeToken != nil {
		// Update the last offset with the resume token
		req.RecordStream.UpdateLatestCheckpointText(base64.StdEncoding.EncodeToString(resumeToken))
		c.logger.Info("[mongo] latest resume token", slog.String("resumeToken", req.LastOffset.Text))
	} else {
		c.logger.Warn("Change stream document does not contain a resume token")
	}

	return nil
}
