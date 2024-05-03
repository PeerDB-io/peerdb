package connectors

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/alerting"
	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	connelasticsearch "github.com/PeerDB-io/peer-flow/connectors/connelasticsearch"
	conneventhub "github.com/PeerDB-io/peer-flow/connectors/eventhub"
	connkafka "github.com/PeerDB-io/peer-flow/connectors/kafka"
	connmysql "github.com/PeerDB-io/peer-flow/connectors/mysql"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connpubsub "github.com/PeerDB-io/peer-flow/connectors/pubsub"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	connsqlserver "github.com/PeerDB-io/peer-flow/connectors/sqlserver"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
)

var ErrUnsupportedFunctionality = errors.New("requested connector does not support functionality")

type Connector interface {
	Close() error
	ConnectionActive(context.Context) error
}

type ValidationConnector interface {
	Connector

	// ValidationCheck performs validation for the connectors,
	// usually includes permissions to create and use objects (tables, schema etc).
	ValidateCheck(context.Context) error
}

type GetTableSchemaConnector interface {
	Connector

	// GetTableSchema returns the schema of a table in terms of QValueKind.
	GetTableSchema(ctx context.Context, req *protos.GetTableSchemaBatchInput) (*protos.GetTableSchemaBatchOutput, error)
}

type CDCPullConnectorCore interface {
	GetTableSchemaConnector

	// EnsurePullability ensures that the connector is pullable.
	EnsurePullability(ctx context.Context, req *protos.EnsurePullabilityBatchInput) (
		*protos.EnsurePullabilityBatchOutput, error)

	// For InitialSnapshotOnly correctness without replication slot
	// `any` is for returning transaction if necessary
	ExportTxSnapshot(context.Context) (*protos.ExportTxSnapshotOutput, any, error)

	// `any` from ExportSnapshot passed here when done, allowing transaction to commit
	FinishExport(any) error

	// Methods related to retrieving and pushing records for this connector as a source and destination.
	SetupReplConn(context.Context) error

	// Ping source to keep connection alive. Can be called concurrently with pulling records; skips ping in that case.
	ReplPing(context.Context) error

	// Called when offset has been confirmed to destination
	UpdateReplStateLastOffset(lastOffset int64)

	// PullFlowCleanup drops both the Postgres publication and replication slot, as a part of DROP MIRROR
	PullFlowCleanup(ctx context.Context, jobName string) error

	// HandleSlotInfo update monitoring info on slot size etc
	HandleSlotInfo(ctx context.Context, alerter *alerting.Alerter,
		catalogPool *pgxpool.Pool, slotName string, peerName string, slotLagGauge *shared.Float64Gauge,
		openConnectionsGauge *shared.Int64Gauge) error

	// GetSlotInfo returns the WAL (or equivalent) info of a slot for the connector.
	GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error)

	// AddTablesToPublication adds additional tables added to a mirror to the publication also
	AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error
}

type CDCPullConnector interface {
	CDCPullConnectorCore

	// This method should be idempotent, and should be able to be called multiple times with the same request.
	PullRecords(ctx context.Context, catalogPool *pgxpool.Pool, req *model.PullRecordsRequest[model.RecordItems]) error
}

type CDCPullPgConnector interface {
	CDCPullConnectorCore

	// This method should be idempotent, and should be able to be called multiple times with the same request.
	// It's signature, aside from type parameter, should match CDCPullConnector.PullRecords.
	PullPg(ctx context.Context, catalogPool *pgxpool.Pool, req *model.PullRecordsRequest[model.PgItems]) error
}

type NormalizedTablesConnector interface {
	Connector

	// StartSetupNormalizedTables may be used to have SetupNormalizedTable calls run in a transaction.
	StartSetupNormalizedTables(ctx context.Context) (any, error)

	// CleanupSetupNormalizedTables may be used to rollback transaction started by StartSetupNormalizedTables.
	// Calling CleanupSetupNormalizedTables after FinishSetupNormalizedTables must be a nop.
	CleanupSetupNormalizedTables(ctx context.Context, tx any)

	// FinishSetupNormalizedTables may be used to finish transaction started by StartSetupNormalizedTables.
	FinishSetupNormalizedTables(ctx context.Context, tx any) error

	// SetupNormalizedTable sets up the normalized table on the connector.
	SetupNormalizedTable(
		ctx context.Context,
		tx any,
		tableIdentifier string,
		tableSchema *protos.TableSchema,
		softDeleteColName string,
		syncedAtColName string,
	) (bool, error)
}

type CDCSyncConnectorCore interface {
	Connector

	// NeedsSetupMetadataTables checks if the metadata table [PEERDB_MIRROR_JOBS] needs to be created.
	NeedsSetupMetadataTables(ctx context.Context) bool

	// SetupMetadataTables creates the metadata table [PEERDB_MIRROR_JOBS] if necessary.
	SetupMetadataTables(ctx context.Context) error

	// GetLastOffset gets the last offset from the metadata table on the destination
	GetLastOffset(ctx context.Context, jobName string) (int64, error)

	// SetLastOffset updates the last offset on the metadata table on the destination
	SetLastOffset(ctx context.Context, jobName string, lastOffset int64) error

	// GetLastSyncBatchID gets the last batch synced to the destination from the metadata table
	GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error)

	// CreateRawTable creates a raw table for the connector with a given name and a fixed schema.
	CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error)

	// SyncFlowCleanup drops metadata tables on the destination, as a part of DROP MIRROR.
	SyncFlowCleanup(ctx context.Context, jobName string) error

	// ReplayTableSchemaDelta changes a destination table to match the schema at source
	// This could involve adding or dropping multiple columns.
	// Connectors which are non-normalizing should implement this as a nop.
	ReplayTableSchemaDeltas(ctx context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error
}

type CDCSyncConnector interface {
	CDCSyncConnectorCore

	// SyncRecords pushes RecordItems to the destination peer and stores it in PeerDB specific tables.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error)
}

type CDCSyncPgConnector interface {
	CDCSyncConnectorCore

	// SyncPg pushes PgItems to the destination peer and stores it in PeerDB specific tables.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	// It's signature, aside from type parameter, should match CDCSyncConnector.SyncRecords.
	SyncPg(ctx context.Context, req *model.SyncRecordsRequest[model.PgItems]) (*model.SyncResponse, error)
}

type CDCNormalizeConnector interface {
	Connector

	// NormalizeRecords merges records pushed earlier into the destination table.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	NormalizeRecords(ctx context.Context, req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error)
}

type QRepPullConnector interface {
	Connector

	// GetQRepPartitions returns the partitions for a given table that haven't been synced yet.
	GetQRepPartitions(ctx context.Context, config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error)

	// PullQRepRecords returns the records for a given partition.
	PullQRepRecords(context.Context, *protos.QRepConfig, *protos.QRepPartition, *model.QRecordStream) (int, error)
}

type QRepSyncConnector interface {
	Connector

	// IsQRepPartitionSynced returns true if a partition has already been synced
	IsQRepPartitionSynced(ctx context.Context, req *protos.IsQRepPartitionSyncedInput) (bool, error)

	// SetupQRepMetadataTables sets up the metadata tables for QRep.
	SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error

	// SyncQRepRecords syncs the records for a given partition.
	// returns the number of records synced.
	SyncQRepRecords(ctx context.Context, config *protos.QRepConfig, partition *protos.QRepPartition,
		stream *model.QRecordStream) (int, error)
}

type QRepConsolidateConnector interface {
	Connector

	// ConsolidateQRepPartitions consolidates the partitions for a given table.
	ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig) error

	// CleanupQRepFlow cleans up the QRep flow for a given table.
	CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error
}

type RenameTablesConnector interface {
	Connector

	RenameTables(context.Context, *protos.RenameTablesInput) (*protos.RenameTablesOutput, error)
}

func GetConnector(ctx context.Context, config *protos.Peer) (Connector, error) {
	switch inner := config.Config.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, inner.PostgresConfig)
	case *protos.Peer_BigqueryConfig:
		return connbigquery.NewBigQueryConnector(ctx, inner.BigqueryConfig)
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, inner.SnowflakeConfig)
	case *protos.Peer_EventhubGroupConfig:
		return conneventhub.NewEventHubConnector(ctx, inner.EventhubGroupConfig)
	case *protos.Peer_S3Config:
		return conns3.NewS3Connector(ctx, inner.S3Config)
	case *protos.Peer_SqlserverConfig:
		return connsqlserver.NewSQLServerConnector(ctx, inner.SqlserverConfig)
	case *protos.Peer_MysqlConfig:
		return connmysql.MySqlConnector{}, nil
	case *protos.Peer_ClickhouseConfig:
		return connclickhouse.NewClickhouseConnector(ctx, inner.ClickhouseConfig)
	case *protos.Peer_KafkaConfig:
		return connkafka.NewKafkaConnector(ctx, inner.KafkaConfig)
	case *protos.Peer_PubsubConfig:
		return connpubsub.NewPubSubConnector(ctx, inner.PubsubConfig)
	case *protos.Peer_ElasticsearchConfig:
		return connelasticsearch.NewElasticsearchConnector(ctx, inner.ElasticsearchConfig)
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetAs[T Connector](ctx context.Context, config *protos.Peer) (T, error) {
	var none T
	conn, err := GetConnector(ctx, config)
	if err != nil {
		return none, err
	}

	if conn, ok := conn.(T); ok {
		return conn, nil
	} else {
		return none, ErrUnsupportedFunctionality
	}
}

func GetCDCPullConnector(ctx context.Context, config *protos.Peer) (CDCPullConnector, error) {
	return GetAs[CDCPullConnector](ctx, config)
}

func GetCDCSyncConnector(ctx context.Context, config *protos.Peer) (CDCSyncConnector, error) {
	return GetAs[CDCSyncConnector](ctx, config)
}

func GetCDCNormalizeConnector(ctx context.Context, config *protos.Peer) (CDCNormalizeConnector, error) {
	return GetAs[CDCNormalizeConnector](ctx, config)
}

func GetQRepPullConnector(ctx context.Context, config *protos.Peer) (QRepPullConnector, error) {
	return GetAs[QRepPullConnector](ctx, config)
}

func GetQRepSyncConnector(ctx context.Context, config *protos.Peer) (QRepSyncConnector, error) {
	return GetAs[QRepSyncConnector](ctx, config)
}

func GetQRepConsolidateConnector(ctx context.Context, config *protos.Peer) (QRepConsolidateConnector, error) {
	return GetAs[QRepConsolidateConnector](ctx, config)
}

func CloseConnector(ctx context.Context, conn Connector) {
	err := conn.Close()
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("error closing connector", slog.Any("error", err))
	}
}

// create type assertions to cause compile time error if connector interface not implemented
var (
	_ CDCPullConnector = &connpostgres.PostgresConnector{}

	_ CDCPullPgConnector = &connpostgres.PostgresConnector{}

	_ CDCSyncConnector = &connpostgres.PostgresConnector{}
	_ CDCSyncConnector = &connbigquery.BigQueryConnector{}
	_ CDCSyncConnector = &connsnowflake.SnowflakeConnector{}
	_ CDCSyncConnector = &conneventhub.EventHubConnector{}
	_ CDCSyncConnector = &connkafka.KafkaConnector{}
	_ CDCSyncConnector = &connpubsub.PubSubConnector{}
	_ CDCSyncConnector = &conns3.S3Connector{}
	_ CDCSyncConnector = &connclickhouse.ClickhouseConnector{}
	_ CDCSyncConnector = &connelasticsearch.ElasticsearchConnector{}

	_ CDCSyncPgConnector = &connpostgres.PostgresConnector{}

	_ CDCNormalizeConnector = &connpostgres.PostgresConnector{}
	_ CDCNormalizeConnector = &connbigquery.BigQueryConnector{}
	_ CDCNormalizeConnector = &connsnowflake.SnowflakeConnector{}
	_ CDCNormalizeConnector = &connclickhouse.ClickhouseConnector{}

	_ GetTableSchemaConnector = &connpostgres.PostgresConnector{}
	_ GetTableSchemaConnector = &connsnowflake.SnowflakeConnector{}

	_ NormalizedTablesConnector = &connpostgres.PostgresConnector{}
	_ NormalizedTablesConnector = &connbigquery.BigQueryConnector{}
	_ NormalizedTablesConnector = &connsnowflake.SnowflakeConnector{}
	_ NormalizedTablesConnector = &connclickhouse.ClickhouseConnector{}

	_ QRepPullConnector = &connpostgres.PostgresConnector{}
	_ QRepPullConnector = &connsqlserver.SQLServerConnector{}

	_ QRepSyncConnector = &connpostgres.PostgresConnector{}
	_ QRepSyncConnector = &connbigquery.BigQueryConnector{}
	_ QRepSyncConnector = &connsnowflake.SnowflakeConnector{}
	_ QRepSyncConnector = &connclickhouse.ClickhouseConnector{}
	_ QRepSyncConnector = &conns3.S3Connector{}
	_ QRepSyncConnector = &connelasticsearch.ElasticsearchConnector{}

	_ QRepConsolidateConnector = &connsnowflake.SnowflakeConnector{}
	_ QRepConsolidateConnector = &connclickhouse.ClickhouseConnector{}

	_ RenameTablesConnector = &connsnowflake.SnowflakeConnector{}
	_ RenameTablesConnector = &connbigquery.BigQueryConnector{}

	_ ValidationConnector = &connsnowflake.SnowflakeConnector{}
	_ ValidationConnector = &connclickhouse.ClickhouseConnector{}
	_ ValidationConnector = &connbigquery.BigQueryConnector{}
	_ ValidationConnector = &conns3.S3Connector{}

	_ Connector = &connmysql.MySqlConnector{}
)
