package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PeerDB-io/peer-flow/alerting"
	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/otel_metrics/peerdb_guages"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

type IcebergConnector struct {
	*metadataStore.PostgresMetadata
	logger         log.Logger
	config         *protos.IcebergConfig
	grpcConnection *grpc.ClientConn
	proxyClient    protos.IcebergProxyServiceClient
}

func (c *IcebergConnector) GetTableSchema(
	ctx context.Context,
	req *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) ExportTxSnapshot(ctx context.Context) (*protos.ExportTxSnapshotOutput, any, error) {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) FinishExport(a any) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) SetupReplConn(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) ReplPing(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) UpdateReplStateLastOffset(lastOffset int64) {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool *pgxpool.Pool,
	slotName string,
	peerName string,
	slotMetricGuages peerdb_guages.SlotMetricGuages,
) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error) {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) PullRecords(
	ctx context.Context,
	catalogPool *pgxpool.Pool,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	// TODO implement me
	panic("implement me")
}

func (c *IcebergConnector) StartSetupNormalizedTables(ctx context.Context) (any, error) {
	// TODO might be better to do all tables in 1 go
	return nil, nil
}

func (c *IcebergConnector) CleanupSetupNormalizedTables(ctx context.Context, tx any) {}

func (c *IcebergConnector) FinishSetupNormalizedTables(ctx context.Context, tx any) error {
	return nil
}

func (c *IcebergConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	err := c.PostgresMetadata.SyncFlowCleanup(ctx, jobName)
	if err != nil {
		return fmt.Errorf("unable to clear metadata for sync flow cleanup : %w", err)
	}
	// TODO implement this
	c.logger.Debug("SyncFlowCleanup for Iceberg is a no-op")
	return nil
}

func (c *IcebergConnector) SetupNormalizedTable(
	ctx context.Context,
	tx any,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
) (bool, error) {
	primaryKeyColumns := make(map[string]struct{}, len(tableSchema.PrimaryKeyColumns))
	for _, col := range tableSchema.PrimaryKeyColumns {
		primaryKeyColumns[col] = struct{}{}
	}

	qFields := make([]qvalue.QField, len(tableSchema.Columns))
	for i, fieldDescription := range tableSchema.Columns {
		colName := fieldDescription.Name
		qValueKind := qvalue.QValueKind(fieldDescription.Type)
		var precision, scale int16
		if qValueKind == qvalue.QValueKindNumeric {
			precision, scale = datatypes.ParseNumericTypmod(fieldDescription.TypeModifier)
		}

		_, isPrimaryKey := primaryKeyColumns[colName]

		qField := qvalue.QField{
			Name:      colName,
			Type:      qValueKind,
			Precision: precision,
			Scale:     scale,
			Nullable:  !isPrimaryKey,
		}
		qFields[i] = qField
	}

	qFields = addPeerMetaColumns(qFields, softDeleteColName, syncedAtColName)

	avroSchema, err := getAvroSchema(tableIdentifier, qvalue.NewQRecordSchema(qFields))
	if err != nil {
		return false, err
	}

	// TODO save to a buffer and call when Finish is called
	// TODO maybe later migrate to a streaming rpc with transaction support
	tableResponse, err := c.proxyClient.CreateTable(ctx, &protos.CreateTableRequest{
		TableInfo: &protos.TableInfo{
			Namespace:      nil,
			TableName:      tableIdentifier,
			IcebergCatalog: c.config.CatalogConfig,
			PrimaryKey:     tableSchema.GetPrimaryKeyColumns(),
		},
		Schema: avroSchema.Schema,
	})
	if err != nil {
		return false, err
	}
	c.logger.Debug("Created iceberg table", slog.String("table", tableResponse.TableName))
	// TODO need to re-enable this and see why it is failing
	// if tableResponse.TableName != tableIdentifier {
	//	return false, fmt.Errorf("created table name mismatch: %s != %s", tableResponse.TableName, tableIdentifier)
	//}
	return true, nil
}

func addPeerMetaColumns(qFields []qvalue.QField, softDeleteColName string, syncedAtColName string) []qvalue.QField {
	qFields = append(qFields,
		qvalue.QField{
			Name:     softDeleteColName,
			Type:     qvalue.QValueKindBoolean,
			Nullable: true,
		}, qvalue.QField{
			Name:     syncedAtColName,
			Type:     qvalue.QValueKindTimestampTZ,
			Nullable: true,
		})
	return qFields
}

func NewIcebergConnector(
	ctx context.Context,
	config *protos.IcebergConfig,
) (*IcebergConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	conn, err := grpc.NewClient(
		peerdbenv.PeerDBFlowJvmAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Iceberg proxy: %w", err)
	}
	client := protos.NewIcebergProxyServiceClient(conn)

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	return &IcebergConnector{
		PostgresMetadata: pgMetadata,
		logger:           logger,
		config:           config,
		grpcConnection:   conn,
		proxyClient:      client,
	}, nil
}

func (c *IcebergConnector) CreateRawTable(_ context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	c.logger.Info("CreateRawTable for Iceberg is a no-op")
	return nil, nil
}

func (c *IcebergConnector) Close() error {
	return c.grpcConnection.Close()
}

func (c *IcebergConnector) ValidateCheck(ctx context.Context) error {
	// Create a table with a random name based on current time
	// TODO ask for namespace in the peer settings and use that instead of __peerdb_test
	// Can also ask for a boolean if provided namespace is to be created or not
	tableName := fmt.Sprintf("__peerdb_test_%d.__peerdb_test_flow_%d", time.Now().Unix(), time.Now().Unix())
	c.logger.Debug("Will try to create iceberg table", "table", tableName)
	_, err := c.proxyClient.CreateTable(ctx,
		&protos.CreateTableRequest{
			TableInfo: &protos.TableInfo{
				Namespace:      nil,
				TableName:      tableName,
				IcebergCatalog: c.config.CatalogConfig,
				PrimaryKey:     nil,
			},
			Schema: `{
  "type": "record",
  "name": "TestObject",
  "namespace": "",
  "fields": [
    {
      "name": "hello",
      "type": [
        "null",
        "int"
      ],
      "default": null
    },
    {
      "name": "some",
      "type": [
        "null",
        "string"
      ],
      "default": null
    }
  ]
}`,
		})
	if err != nil {
		return err
	}
	c.logger.Debug("Created iceberg table, will try to drop it now", "table", tableName)
	dropTable, err := c.proxyClient.DropTable(ctx,
		&protos.DropTableRequest{
			TableInfo: &protos.TableInfo{
				Namespace:      nil,
				TableName:      tableName,
				IcebergCatalog: c.config.CatalogConfig,
				PrimaryKey:     nil,
			},
			Purge: true,
		},
	)
	if err != nil {
		return err
	}
	if !dropTable.Success {
		return fmt.Errorf("failed to drop table %s", tableName)
	}
	c.logger.Debug("Dropped iceberg table", slog.String("table", tableName))
	return nil
}

func (c *IcebergConnector) ConnectionActive(ctx context.Context) error {
	// TODO implement this for iceberg
	return nil
}

func (c *IcebergConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		c.logger.Error("failed to increment id", "error", err)
		return nil, err
	}
	numRecords := 0
	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       int64(numRecords),
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *IcebergConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	c.logger.Info("ReplayTableSchemaDeltas for Iceberg is a no-op")
	return nil
}
