package connmongo

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerdb_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	DefaultDocumentKeyColumnName  = "_id"
	DefaultFullDocumentColumnName = "_full_document"
)

type MongoConnector struct {
	*metadataStore.PostgresMetadata
	config    *protos.MongoConfig
	client    *mongo.Client
	logger    log.Logger
	bytesRead atomic.Int64
}

func NewMongoConnector(ctx context.Context, config *protos.MongoConfig) (*MongoConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	clientOptions, err := parseAsClientOptions(config)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(clientOptions)
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
	buildInfo, err := peerdb_mongo.GetBuildInfo(ctx, c.client)
	if err != nil {
		return "", err
	}
	return buildInfo.Version, nil
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
		SetFullDocumentBeforeChange(options.Off)
	changeStream, err := c.client.Watch(ctx, defaultPipeline(), changeStreamOpts)
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
		SetFullDocumentBeforeChange(options.Off)
	if req.LastOffset.Text != "" {
		// If we have a last offset, we resume from that point
		c.logger.Info("[mongo] resuming change stream", slog.String("resumeToken", req.LastOffset.Text))
		resumeTokenBytes, err := base64.StdEncoding.DecodeString(req.LastOffset.Text)
		if err != nil {
			return fmt.Errorf("failed to parse last offset: %w", err)
		}
		changeStreamOpts.SetResumeAfter(bson.Raw(resumeTokenBytes))
	}

	changeStream, err := c.client.Watch(ctx, defaultPipeline(), changeStreamOpts)
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

		if _, ok := changeDoc["operationType"]; !ok {
			c.logger.Warn("operationType field not found")
			continue
		}

		clusterTime := changeDoc["clusterTime"].(bson.Timestamp)
		clusterTimeNanos := time.Unix(int64(clusterTime.T), 0).UnixNano()

		sourceTableName := fmt.Sprintf("%s.%s", changeDoc["ns"].(bson.D)[0].Value, changeDoc["ns"].(bson.D)[1].Value)
		destinationTableName := req.TableNameMapping[sourceTableName].Name

		items := model.NewMongoRecordItems(2)

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

func defaultPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}},
			}},
		}}},

		// Mongo recommend using '$project' first to reduce change event size, and only use
		// '$changeStreamSplitLargeEvent' in the pipeline if still necessary. Given the document
		// themselves have a 16MB limit, project required fields for now for code simplicity.
		// ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStreamSplitLargeEvent/
		{{Key: "$project", Value: bson.D{
			{Key: "operationType", Value: 1},
			{Key: "clusterTime", Value: 1},
			{Key: "documentKey", Value: 1},
			{Key: "fullDocument", Value: 1},
			{Key: "ns", Value: 1},
		}}},
	}
}

func parseAsClientOptions(config *protos.MongoConfig) (*options.ClientOptions, error) {
	connStr, err := connstring.Parse(config.Uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing uri: %w", err)
	}

	if connStr.Username != "" || connStr.Password != "" {
		return nil, errors.New("connection string should not contain username and password")
	}

	clientOptions := options.Client().
		ApplyURI(config.Uri).
		SetAppName("PeerDB Mongo Connector").
		SetAuth(options.Credential{
			Username: config.Username,
			Password: config.Password,
		}).
		// always use compression
		SetCompressors([]string{"zstd", "snappy"}).
		// always use majority read concern for correctness
		SetReadConcern(readconcern.Majority())

	// allow user to override with other read preference
	if connStr.ReadPreference == "" {
		clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	}

	if !config.DisableTls {
		tlsConfig, err := shared.CreateTlsConfig(tls.VersionTLS12, config.RootCa, "", config.TlsHost, false)
		if err != nil {
			return nil, err
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	err = clientOptions.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating client options: %w", err)
	}
	return clientOptions, nil
}
