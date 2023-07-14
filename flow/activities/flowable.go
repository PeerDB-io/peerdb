package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	log "github.com/sirupsen/logrus"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	// True of metadata tables need to be set up.
	NeedsSetupMetadataTables bool
}

// IFlowable is a collection of activities that deal with flowables.
// Flowables are entities that can be used as a source or destination in a peer flow.
type IFlowable interface {
	// CheckConnection checks the connection to the flowable.
	CheckConnection(ctx context.Context, config *protos.Peer) (CheckConnectionResult, error)
	// SetupMetadataTables sets up the metadata tables for the flowable.
	SetupMetadataTables(ctx context.Context, config *protos.Peer) error
	// GetLastSyncedID returns the last synced ID for the flowable.
	// This typically corresponds to the LastLSN for Postgres and similar for other databases.
	GetLastSyncedID(ctx context.Context, config *protos.GetLastSyncedIDInput) (*protos.LastSyncState, error)
	// EnsurePullability ensurses that the flowable is pullable, i.e, table exists and requisite
	// slots and publications are set up.
	EnsurePullability(ctx context.Context, config *protos.EnsurePullabilityInput) error
	// CreateRawTable creates the raw table on the flowable.
	CreateRawTable(
		ctx context.Context,
		config *protos.CreateRawTableInput,
	) (*protos.CreateRawTableOutput, error)
	// Normalization Setup Methods
	// GetTableSchema returns the schema of a table.
	GetTableSchema(ctx context.Context, config *protos.GetTableSchemaInput) (*protos.TableSchema, error)
	// CreateNormalizedTable sets up the normalized table on the flowable.
	CreateNormalizedTable(ctx context.Context,
		config *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error)
	// StartFlow starts the flow of events from the source to the destination flowable.
	StartFlow(ctx context.Context, input *protos.StartFlowInput) error

	////////// QRep Methods //////////

	// SetupQRepMetadataTables sets up the QRep metadata tables for the flowable.
	SetupQRepMetadataTables(ctx context.Context, config *protos.Peer) error

	// GetQRepPartitions returns the partitions for a given QRepConfig.
	GetQRepPartitions(ctx context.Context, config *protos.QRepConfig) ([]*protos.QRepPartition, error)

	// ReplicateQRepPartition replicates a QRepPartition from the source to the destination.
	ReplicateQRepPartition(ctx context.Context, partition *protos.QRepPartition) error

	// ConsolidateQRepPartitions consolidates the QRepPartitions into the destination.
	ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig) error

	// CleanupQrepFlow cleans up the QRep flow.
	CleanupQrepFlow(ctx context.Context, config *protos.QRepConfig) error

	DropFlow(ctx context.Context, config *protos.DropFlowInput) error
}

// FlowableActivity is the activity implementation for IFlowable.
type FlowableActivity struct{}

// CheckConnection implements IFlowable.CheckConnection.
func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.Peer,
) (*CheckConnectionResult, error) {
	conn, err := connectors.GetConnector(ctx, config)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	needsSetup := conn.NeedsSetupMetadataTables()

	return &CheckConnectionResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

// SetupMetadataTables implements IFlowable.SetupMetadataTables.
func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.Peer) error {
	conn, err := connectors.GetConnector(ctx, config)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}

	if err := conn.SetupMetadataTables(); err != nil {
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetLastSyncedID implements IFlowable.GetLastSyncedID.
func (a *FlowableActivity) GetLastSyncedID(
	ctx context.Context,
	config *protos.GetLastSyncedIDInput,
) (*protos.LastSyncState, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.GetLastOffset(config.FlowJobName)
}

// EnsurePullability implements IFlowable.EnsurePullability.
func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	relID, err := conn.EnsurePullability(config)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	return relID, nil
}

func (a *FlowableActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) error {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}

	err = conn.SetupReplication(config)
	if err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	return nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.CreateRawTable(config)
}

// GetTableSchema returns the schema of a table.
func (a *FlowableActivity) GetTableSchema(
	ctx context.Context,
	config *protos.GetTableSchemaInput,
) (*protos.TableSchema, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.GetTableSchema(config)
}

// CreateNormalizedTable creates a normalized table in the destination flowable.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableInput,
) (*protos.SetupNormalizedTableOutput, error) {
	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
	defer connectors.CloseConnector(conn)

	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	return conn.SetupNormalizedTable(config)
}

// StartFlow implements IFlowable.StartFlow.
func (a *FlowableActivity) StartFlow(ctx context.Context, input *protos.StartFlowInput) (*model.SyncResponse, error) {
	conn := input.FlowConnectionConfigs

	src, err := connectors.GetConnector(ctx, conn.Source)
	defer connectors.CloseConnector(src)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, conn.Destination)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}

	log.Info("initializing table schema...")
	err = dest.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	log.Info("pulling records...")

	records, err := src.PullRecords(&model.PullRecordsRequest{
		FlowJobName:            input.FlowConnectionConfigs.FlowJobName,
		SrcTableIDNameMapping:  input.FlowConnectionConfigs.SrcTableIdNameMapping,
		TableNameMapping:       input.FlowConnectionConfigs.TableNameMapping,
		LastSyncState:          input.LastSyncState,
		MaxBatchSize:           uint32(input.SyncFlowOptions.BatchSize),
		IdleTimeout:            10 * time.Second,
		TableNameSchemaMapping: input.FlowConnectionConfigs.TableNameSchemaMapping,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to pull records: %w", err)
	}

	// log the number of records
	numRecords := len(records.Records)
	log.Printf("pulled %d records", numRecords)

	if numRecords == 0 {
		log.Info("no records to push")
		return nil, nil
	}

	res, err := dest.SyncRecords(&model.SyncRecordsRequest{
		Records:     records,
		FlowJobName: input.FlowConnectionConfigs.FlowJobName,
	})

	log.Info("pushed records")

	if err != nil {
		return nil, fmt.Errorf("failed to push records: %w", err)
	}

	return res, nil
}

func (a *FlowableActivity) StartNormalize(ctx context.Context, input *protos.StartNormalizeInput) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs

	src, err := connectors.GetConnector(ctx, conn.Source)
	defer connectors.CloseConnector(src)
	if err != nil {
		return nil, fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, conn.Destination)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination connector: %w", err)
	}

	log.Info("initializing table schema...")
	err = dest.InitializeTableSchema(input.FlowConnectionConfigs.TableNameSchemaMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table schema: %w", err)
	}

	res, err := dest.NormalizeRecords(&model.NormalizeRecordsRequest{
		FlowJobName: input.FlowConnectionConfigs.FlowJobName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to normalized records: %w", err)
	}

	// log the number of batches normalized
	if res != nil {
		log.Printf("normalized records from batch %d to batch %d\n", res.StartBatchID, res.EndBatchID)
	}

	return res, nil
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	return conn.SetupQRepMetadataTables(config)
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) (*protos.QRepParitionResult, error) {
	conn, err := connectors.GetConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(conn)

	partitions, err := conn.GetQRepPartitions(config, last)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions from source: %w", err)
	}

	return &protos.QRepParitionResult{
		Partitions: partitions,
	}, nil
}

// ReplicateQRepPartition replicates a QRepPartition from the source to the destination.
func (a *FlowableActivity) ReplicateQRepPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
) error {
	srcConn, err := connectors.GetConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(srcConn)

	destConn, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(destConn)

	log.Printf("replicating partition %s\n", partition.PartitionId)

	recordBatch, err := srcConn.PullQRepRecords(config, partition)
	if err != nil {
		return fmt.Errorf("failed to pull records: %w", err)
	}

	log.Printf("pulled %d records\n", len(recordBatch.Records))

	res, err := destConn.SyncQRepRecords(config, partition, recordBatch)
	if err != nil {
		return fmt.Errorf("failed to sync records: %w", err)
	}

	log.Printf("pushed %d records\n", res)
	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig) error {
	dst, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	return dst.ConsolidateQRepPartitions(config)
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	dst, err := connectors.GetConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	return dst.CleanupQRepFlow(config)
}

func (a *FlowableActivity) DropFlow(ctx context.Context, config *protos.ShutdownRequest) error {
	src, err := connectors.GetConnector(ctx, config.SourcePeer)
	defer connectors.CloseConnector(src)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}

	dest, err := connectors.GetConnector(ctx, config.DestinationPeer)
	defer connectors.CloseConnector(dest)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}

	err = src.PullFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup source: %w", err)
	}
	err = dest.SyncFlowCleanup(config.FlowJobName)
	if err != nil {
		return fmt.Errorf("failed to cleanup destination: %w", err)
	}
	return nil
}
