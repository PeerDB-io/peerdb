package connectors

import (
	"context"
	"fmt"

	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	conneventhub "github.com/PeerDB-io/peer-flow/connectors/eventhub"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	connsqlserver "github.com/PeerDB-io/peer-flow/connectors/sqlserver"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
)

type Connector interface {
	Close() error
	ConnectionActive() bool
	NeedsSetupMetadataTables() bool
	SetupMetadataTables() error
	GetLastOffset(jobName string) (*protos.LastSyncState, error)

	// GetTableSchema returns the schema of a table.
	GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error)

	// SetupNormalizedTable sets up the normalized table on the connector.
	SetupNormalizedTable(req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error)

	// EnsurePullability ensures that the connector is pullable.
	EnsurePullability(req *protos.EnsurePullabilityInput) (*protos.EnsurePullabilityOutput, error)

	// InitializeTableSchema initializes the table schema of all the destination tables for the connector.
	InitializeTableSchema(req map[string]*protos.TableSchema) error

	// Methods related to retrieving and pusing records for this connector as a source and destination.

	// PullRecords pulls records from the source, and returns a RecordBatch.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error)

	// SyncRecords pushes records to the destination peer and stores it in PeerDB specific tables.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error)

	// NormalizeRecords merges records pushed earlier into the destination table.
	// This method should be idempotent, and should be able to be called multiple times with the same request.
	NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error)

	// CreateRawTable creates a raw table for the connector with a given name and a fixed schema.
	CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error)

	///// QRep methods /////

	// SetupQRepMetadataTables sets up the metadata tables for QRep.
	SetupQRepMetadataTables(config *protos.QRepConfig) error

	// GetQRepPartitions returns the partitions for a given table that haven't been synced yet.
	GetQRepPartitions(config *protos.QRepConfig, last *protos.QRepPartition) ([]*protos.QRepPartition, error)

	// GetQRepRecords returns the records for a given partition.
	PullQRepRecords(config *protos.QRepConfig, partition *protos.QRepPartition) (*model.QRecordBatch, error)

	// SyncQRepRecords syncs the records for a given partition.
	// returns the number of records synced.
	SyncQRepRecords(config *protos.QRepConfig, partition *protos.QRepPartition, records *model.QRecordBatch) (int, error)

	// ConsolidateQRepPartitions consolidates the partitions for a given table.
	ConsolidateQRepPartitions(config *protos.QRepConfig) error

	// CleanupQRepFlow cleans up the QRep flow for a given table.
	CleanupQRepFlow(config *protos.QRepConfig) error

	PullFlowCleanup(jobName string) error
	SyncFlowCleanup(jobName string) error
}

func GetConnector(ctx context.Context, config *protos.Peer) (Connector, error) {
	inner := config.Config
	switch inner.(type) {
	case *protos.Peer_PostgresConfig:
		return connpostgres.NewPostgresConnector(ctx, config.GetPostgresConfig())
	case *protos.Peer_BigqueryConfig:
		return connbigquery.NewBigQueryConnector(ctx, config.GetBigqueryConfig())
	case *protos.Peer_SnowflakeConfig:
		return connsnowflake.NewSnowflakeConnector(ctx, config.GetSnowflakeConfig())
	case *protos.Peer_EventhubConfig:
		return conneventhub.NewEventHubConnector(ctx, config.GetEventhubConfig())
	case *protos.Peer_S3Config:
		return conns3.NewS3Connector(ctx, config.GetS3Config())
	case *protos.Peer_SqlserverConfig:
		return connsqlserver.NewSQLServerConnector(ctx, config.GetSqlserverConfig())
	default:
		return nil, fmt.Errorf("requested connector is not yet implemented")
	}
}

func CloseConnector(conn Connector) {
	if conn == nil {
		return
	}

	conn.Close()
}
