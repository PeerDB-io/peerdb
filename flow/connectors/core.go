package connectors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"

	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	conneventhub "github.com/PeerDB-io/peer-flow/connectors/eventhub"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	connsqlserver "github.com/PeerDB-io/peer-flow/connectors/sqlserver"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrUnsupportedFunctionality = errors.New("requested connector does not support functionality")

type Connector interface {
	Close() error
	ConnectionActive() error
}

type CDCPullConnector interface {
	Connector

	// GetTableSchema returns the schema of a table.
	GetTableSchema(req *protos.GetTableSchemaBatchInput) (*protos.GetTableSchemaBatchOutput, error)

	// EnsurePullability ensures that the connector is pullable.
	EnsurePullability(req *protos.EnsurePullabilityBatchInput) (
		*protos.EnsurePullabilityBatchOutput, error)

	// Methods related to retrieving and pusing records for this connector as a source and destination.

	// PullRecords pulls records from the source, and returns a RecordBatch.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	PullRecords(catalogPool *pgxpool.Pool, req *model.PullRecordsRequest) error

	// PullFlowCleanup drops both the Postgres publication and replication slot, as a part of DROP MIRROR
	PullFlowCleanup(jobName string) error

	// SendWALHeartbeat allows for activity to progress restart_lsn on postgres.
	SendWALHeartbeat() error

	// GetSlotInfo returns the WAL (or equivalent) info of a slot for the connector.
	GetSlotInfo(slotName string) ([]*protos.SlotInfo, error)
}

type CDCSyncConnector interface {
	Connector

	// NeedsSetupMetadataTables checks if the metadata table [PEERDB_MIRROR_JOBS] needs to be created.
	NeedsSetupMetadataTables() bool

	// SetupMetadataTables creates the metadata table [PEERDB_MIRROR_JOBS] if necessary.
	SetupMetadataTables() error

	// GetLastOffset gets the last offset from the metadata table on the destination
	GetLastOffset(jobName string) (int64, error)

	// GetLastSyncBatchID gets the last batch synced to the destination from the metadata table
	GetLastSyncBatchID(jobName string) (int64, error)

	// InitializeTableSchema initializes the table schema of all the destination tables for the connector.
	InitializeTableSchema(req map[string]*protos.TableSchema) error

	// CreateRawTable creates a raw table for the connector with a given name and a fixed schema.
	CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error)

	// SetupNormalizedTables sets up the normalized table on the connector.
	SetupNormalizedTables(req *protos.SetupNormalizedTableBatchInput) (
		*protos.SetupNormalizedTableBatchOutput, error)

	// SyncRecords pushes records to the destination peer and stores it in PeerDB specific tables.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error)

	// SyncFlowCleanup drops metadata tables on the destination, as a part of DROP MIRROR.
	SyncFlowCleanup(jobName string) error
}

type CDCNormalizeConnector interface {
	Connector

	// InitializeTableSchema initializes the table schema of all the destination tables for the connector.
	InitializeTableSchema(req map[string]*protos.TableSchema) error

	// NormalizeRecords merges records pushed earlier into the destination table.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error)

	// ReplayTableSchemaDelta changes a destination table to match the schema at source
	// This could involve adding or dropping multiple columns.
	ReplayTableSchemaDeltas(flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error
}

type QRepPullConnector interface {
	Connector

	// GetQRepPartitions returns the partitions for a given table that haven't been synced yet.
	GetQRepPartitions(config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error)

	// PullQRepRecords returns the records for a given partition.
	PullQRepRecords(config *protos.QRepConfig, partition *protos.QRepPartition) (*model.QRecordBatch, error)
}

type QRepSyncConnector interface {
	Connector

	// SetupQRepMetadataTables sets up the metadata tables for QRep.
	SetupQRepMetadataTables(config *protos.QRepConfig) error

	// SyncQRepRecords syncs the records for a given partition.
	// returns the number of records synced.
	SyncQRepRecords(config *protos.QRepConfig, partition *protos.QRepPartition,
		stream *model.QRecordStream) (int, error)
}

type QRepConsolidateConnector interface {
	Connector

	// ConsolidateQRepPartitions consolidates the partitions for a given table.
	ConsolidateQRepPartitions(config *protos.QRepConfig) error

	// CleanupQRepFlow cleans up the QRep flow for a given table.
	CleanupQRepFlow(config *protos.QRepConfig) error
}

func GetCDCPullConnector(ctx context.Context, config *protos.Peer) (CDCPullConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetCDCSyncConnector(ctx context.Context, config *protos.Peer) (CDCSyncConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	case *protos.Peer_BigqueryConfig:
		return connbigquery.NewBigQueryConnector(ctx, config.GetBigqueryConfig())
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, config.GetSnowflakeConfig())
	case *protos.Peer_EventhubConfig:
		return nil, fmt.Errorf("use eventhub group config instead")
	case *protos.Peer_EventhubGroupConfig:
		return conneventhub.NewEventHubConnector(ctx, config.GetEventhubGroupConfig())
	case *protos.Peer_S3Config:
		return conns3.NewS3Connector(ctx, config.GetS3Config())
	case *protos.Peer_ClickhouseConfig:
		return connclickhouse.NewClickhouseConnector(ctx, config.GetClickhouseConfig())
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetCDCNormalizeConnector(ctx context.Context,
	config *protos.Peer,
) (CDCNormalizeConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	case *protos.Peer_BigqueryConfig:
		return connbigquery.NewBigQueryConnector(ctx, config.GetBigqueryConfig())
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, config.GetSnowflakeConfig())
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetQRepPullConnector(ctx context.Context, config *protos.Peer) (QRepPullConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	case *protos.Peer_SqlserverConfig:
		return connsqlserver.NewSQLServerConnector(ctx, config.GetSqlserverConfig())
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetQRepSyncConnector(ctx context.Context, config *protos.Peer) (QRepSyncConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	case *protos.Peer_BigqueryConfig:
		return connbigquery.NewBigQueryConnector(ctx, config.GetBigqueryConfig())
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, config.GetSnowflakeConfig())
	case *protos.Peer_S3Config:
		return conns3.NewS3Connector(ctx, config.GetS3Config())
	case *protos.Peer_ClickhouseConfig:
		return connclickhouse.NewClickhouseConnector(ctx, config.GetClickhouseConfig())
	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func GetConnector(ctx context.Context, peer *protos.Peer) (Connector, error) {
	inner := peer.Type
	switch inner {
	case protos.DBType_POSTGRES:
		pgConfig := peer.GetPostgresConfig()

		if pgConfig == nil {
			return nil, fmt.Errorf("missing postgres config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return connpostgres.NewPostgresConnector(ctx, pgConfig)
	case protos.DBType_BIGQUERY:
		bqConfig := peer.GetBigqueryConfig()
		if bqConfig == nil {
			return nil, fmt.Errorf("missing bigquery config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return connbigquery.NewBigQueryConnector(ctx, bqConfig)

	case protos.DBType_SNOWFLAKE:
		sfConfig := peer.GetSnowflakeConfig()
		if sfConfig == nil {
			return nil, fmt.Errorf("missing snowflake config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return connsnowflake.NewSnowflakeConnector(ctx, sfConfig)
	case protos.DBType_SQLSERVER:
		sqlServerConfig := peer.GetSqlserverConfig()
		if sqlServerConfig == nil {
			return nil, fmt.Errorf("missing sqlserver config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return connsqlserver.NewSQLServerConnector(ctx, sqlServerConfig)
	case protos.DBType_S3:
		s3Config := peer.GetS3Config()
		if s3Config == nil {
			return nil, fmt.Errorf("missing s3 config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return conns3.NewS3Connector(ctx, s3Config)
	case protos.DBType_CLICKHOUSE:
		clickhouseConfig := peer.GetClickhouseConfig()
		if clickhouseConfig == nil {
			return nil, fmt.Errorf("missing clickhouse config for %s peer %s", peer.Type.String(), peer.Name)
		}
		return connclickhouse.NewClickhouseConnector(ctx, clickhouseConfig)
	// case protos.DBType_EVENTHUB:
	// 	return connsqlserver.NewSQLServerConnector(ctx, config.GetSqlserverConfig())
	default:
		return nil, fmt.Errorf("unsupported peer type %s", peer.Type.String())
	}
}

func GetQRepConsolidateConnector(ctx context.Context,
	config *protos.Peer,
) (QRepConsolidateConnector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, config.GetSnowflakeConfig())

	default:
		return nil, ErrUnsupportedFunctionality
	}
}

func CloseConnector(conn Connector) {
	if conn == nil {
		return
	}

	err := conn.Close()
	if err != nil {
		slog.Error("error closing connector", slog.Any("error", err))
	}
}
