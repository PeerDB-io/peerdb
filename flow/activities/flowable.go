package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors"
	connbigquery "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	connsnowflake "github.com/PeerDB-io/peer-flow/connectors/snowflake"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

// CheckConnectionResult is the result of a CheckConnection call.
type CheckConnectionResult struct {
	NeedsSetupMetadataTables bool
}

type CdcCacheEntry struct {
	connector connectors.CDCPullConnectorCore
	done      chan struct{}
}

type OtelManager struct {
	MetricsProvider            *sdkmetric.MeterProvider
	SlotLagMeter               metric.Meter
	OpenConnectionsMeter       metric.Meter
	SlotLagGaugesCache         map[string]*shared.Float64Gauge
	OpenConnectionsGaugesCache map[string]*shared.Int64Gauge
}

type FlowableActivity struct {
	CatalogPool *pgxpool.Pool
	Alerter     *alerting.Alerter
	CdcCache    map[string]CdcCacheEntry
	OtelManager *OtelManager
	CdcCacheRw  sync.RWMutex
}

func (a *FlowableActivity) CheckConnection(
	ctx context.Context,
	config *protos.SetupInput,
) (*CheckConnectionResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	needsSetup := dstConn.NeedsSetupMetadataTables(ctx)

	return &CheckConnectionResult{
		NeedsSetupMetadataTables: needsSetup,
	}, nil
}

func (a *FlowableActivity) SetupMetadataTables(ctx context.Context, config *protos.SetupInput) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.Peer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if err := dstConn.SetupMetadataTables(ctx); err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowName, err)
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

func (a *FlowableActivity) EnsurePullability(
	ctx context.Context,
	config *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	output, err := srcConn.EnsurePullability(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	return output, nil
}

// CreateRawTable creates a raw table in the destination flowable.
func (a *FlowableActivity) CreateRawTable(
	ctx context.Context,
	config *protos.CreateRawTableInput,
) (*protos.CreateRawTableOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	res, err := dstConn.CreateRawTable(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, err
	}
	err = monitoring.InitializeCDCFlow(ctx, a.CatalogPool, config.FlowJobName)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// GetTableSchema returns the schema of a table.
func (a *FlowableActivity) GetTableSchema(
	ctx context.Context,
	config *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	srcConn, err := connectors.GetAs[connectors.GetTableSchemaConnector](ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get CDCPullPgConnector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	return srcConn.GetTableSchema(ctx, config)
}

// CreateNormalizedTable creates normalized tables in destination.
func (a *FlowableActivity) CreateNormalizedTable(
	ctx context.Context,
	config *protos.SetupNormalizedTableBatchInput,
) (*protos.SetupNormalizedTableBatchOutput, error) {
	logger := activity.GetLogger(ctx)
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowName)
	conn, err := connectors.GetAs[connectors.NormalizedTablesConnector](ctx, config.PeerConnectionConfig)
	if err != nil {
		if err == connectors.ErrUnsupportedFunctionality {
			logger.Info("Connector does not implement normalized tables")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	tx, err := conn.StartSetupNormalizedTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup normalized tables tx: %w", err)
	}
	defer conn.CleanupSetupNormalizedTables(ctx, tx)

	numTablesSetup := atomic.Uint32{}
	totalTables := uint32(len(config.TableNameSchemaMapping))
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("setting up normalized tables - %d of %d done",
			numTablesSetup.Load(), totalTables)
	})
	defer shutdown()

	tableExistsMapping := make(map[string]bool)
	for tableIdentifier, tableSchema := range config.TableNameSchemaMapping {
		var existing bool
		existing, err = conn.SetupNormalizedTable(
			ctx,
			tx,
			tableIdentifier,
			tableSchema,
			config.SoftDeleteColName,
			config.SyncedAtColName,
		)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowName, err)
			return nil, fmt.Errorf("failed to setup normalized table %s: %w", tableIdentifier, err)
		}
		tableExistsMapping[tableIdentifier] = existing

		numTablesSetup.Add(1)
		if !existing {
			logger.Info("created table " + tableIdentifier)
		} else {
			logger.Info("table already exists " + tableIdentifier)
		}
	}

	if err := conn.FinishSetupNormalizedTables(ctx, tx); err != nil {
		return nil, fmt.Errorf("failed to commit normalized tables tx: %w", err)
	}

	return &protos.SetupNormalizedTableBatchOutput{
		TableExistsMapping: tableExistsMapping,
	}, nil
}

func (a *FlowableActivity) MaintainPull(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	sessionID string,
) error {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
	if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, srcConn)

	if err := srcConn.SetupReplConn(ctx); err != nil {
		return err
	}

	done := make(chan struct{})
	a.CdcCacheRw.Lock()
	a.CdcCache[sessionID] = CdcCacheEntry{
		connector: srcConn,
		done:      done,
	}
	a.CdcCacheRw.Unlock()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "keep session alive")
			if err := srcConn.ReplPing(ctx); err != nil {
				a.CdcCacheRw.Lock()
				delete(a.CdcCache, sessionID)
				a.CdcCacheRw.Unlock()
				return temporal.NewNonRetryableApplicationError("connection to source down", "disconnect", err)
			}
		case <-done:
			return nil
		case <-ctx.Done():
			a.CdcCacheRw.Lock()
			delete(a.CdcCache, sessionID)
			a.CdcCacheRw.Unlock()
			return nil
		}
	}
}

func (a *FlowableActivity) UnmaintainPull(ctx context.Context, sessionID string) error {
	a.CdcCacheRw.Lock()
	if entry, ok := a.CdcCache[sessionID]; ok {
		close(entry.done)
		delete(a.CdcCache, sessionID)
	}
	a.CdcCacheRw.Unlock()
	return nil
}

func (a *FlowableActivity) SyncRecords(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	sessionID string,
) (*model.SyncResponse, error) {
	return syncCore(ctx, a, config, options, sessionID,
		connectors.CDCPullConnector.PullRecords,
		connectors.CDCSyncConnector.SyncRecords)
}

func (a *FlowableActivity) SyncPg(
	ctx context.Context,
	config *protos.FlowConnectionConfigs,
	options *protos.SyncFlowOptions,
	sessionID string,
) (*model.SyncResponse, error) {
	return syncCore(ctx, a, config, options, sessionID,
		connectors.CDCPullPgConnector.PullPg,
		connectors.CDCSyncPgConnector.SyncPg)
}

func (a *FlowableActivity) StartNormalize(
	ctx context.Context,
	input *protos.StartNormalizeInput,
) (*model.NormalizeResponse, error) {
	conn := input.FlowConnectionConfigs
	ctx = context.WithValue(ctx, shared.FlowNameKey, conn.FlowJobName)
	logger := activity.GetLogger(ctx)

	dstConn, err := connectors.GetCDCNormalizeConnector(ctx, conn.Destination)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		err = monitoring.UpdateEndTimeForCDCBatch(ctx, a.CatalogPool, input.FlowConnectionConfigs.FlowJobName,
			input.SyncBatchID)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "normalizing records from batch for job"
	})
	defer shutdown()

	res, err := dstConn.NormalizeRecords(ctx, &model.NormalizeRecordsRequest{
		FlowJobName:            input.FlowConnectionConfigs.FlowJobName,
		SyncBatchID:            input.SyncBatchID,
		SoftDelete:             input.FlowConnectionConfigs.SoftDelete,
		SoftDeleteColName:      input.FlowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:        input.FlowConnectionConfigs.SyncedAtColName,
		TableNameSchemaMapping: input.TableNameSchemaMapping,
	})
	if err != nil {
		a.Alerter.LogFlowError(ctx, input.FlowConnectionConfigs.FlowJobName, err)
		return nil, fmt.Errorf("failed to normalized records: %w", err)
	}

	// normalize flow did not run due to no records, no need to update end time.
	if res.Done {
		err = monitoring.UpdateEndTimeForCDCBatch(
			ctx,
			a.CatalogPool,
			input.FlowConnectionConfigs.FlowJobName,
			res.EndBatchID,
		)
		if err != nil {
			return nil, err
		}
	}

	// log the number of batches normalized
	logger.Info(fmt.Sprintf("normalized records from batch %d to batch %d",
		res.StartBatchID, res.EndBatchID))

	return res, nil
}

// SetupQRepMetadataTables sets up the metadata tables for QReplication.
func (a *FlowableActivity) SetupQRepMetadataTables(ctx context.Context, config *protos.QRepConfig) error {
	conn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	err = conn.SetupQRepMetadataTables(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return fmt.Errorf("failed to setup metadata tables: %w", err)
	}

	return nil
}

// GetQRepPartitions returns the partitions for a given QRepConfig.
func (a *FlowableActivity) GetQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
	runUUID string,
) (*protos.QRepParitionResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	err := monitoring.InitializeQRepRun(ctx, a.CatalogPool, config, runUUID, nil)
	if err != nil {
		return nil, err
	}
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get qrep pull connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "getting partitions for job"
	})
	defer shutdown()
	partitions, err := srcConn.GetQRepPartitions(ctx, config, last)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to get partitions from source: %w", err)
	}
	if len(partitions) > 0 {
		err = monitoring.InitializeQRepRun(
			ctx,
			a.CatalogPool,
			config,
			runUUID,
			partitions,
		)
		if err != nil {
			return nil, err
		}
	}

	return &protos.QRepParitionResult{
		Partitions: partitions,
	}, nil
}

// ReplicateQRepPartitions spawns multiple ReplicateQRepPartition
func (a *FlowableActivity) ReplicateQRepPartitions(ctx context.Context,
	config *protos.QRepConfig,
	partitions *protos.QRepPartitionBatch,
	runUUID string,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(activity.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))
	err := monitoring.UpdateStartTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	if err != nil {
		return fmt.Errorf("failed to update start time for qrep run: %w", err)
	}

	numPartitions := len(partitions.Partitions)

	logger.Info(fmt.Sprintf("replicating partitions for batch %d - size: %d",
		partitions.BatchId, numPartitions),
	)
	for i, p := range partitions.Partitions {
		logger.Info(fmt.Sprintf("batch-%d - replicating partition - %s", partitions.BatchId, p.PartitionId))
		err := a.replicateQRepPartition(ctx, config, i+1, numPartitions, p, runUUID)
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return err
		}
	}

	return nil
}

func (a *FlowableActivity) ConsolidateQRepPartitions(ctx context.Context, config *protos.QRepConfig,
	runUUID string,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
	} else if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, dstConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "consolidating partitions for job"
	})
	defer shutdown()

	err = dstConn.ConsolidateQRepPartitions(ctx, config)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return err
	}

	return monitoring.UpdateEndTimeForQRepRun(ctx, a.CatalogPool, runUUID)
}

func (a *FlowableActivity) CleanupQRepFlow(ctx context.Context, config *protos.QRepConfig) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dst, err := connectors.GetQRepConsolidateConnector(ctx, config.DestinationPeer)
	if errors.Is(err, connectors.ErrUnsupportedFunctionality) {
		return nil
	} else if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return err
	}
	defer dst.Close()

	return dst.CleanupQRepFlow(ctx, config)
}

func (a *FlowableActivity) DropFlowSource(ctx context.Context, config *protos.ShutdownRequest) error {
	srcConn, err := connectors.GetCDCPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	return srcConn.PullFlowCleanup(ctx, config.FlowJobName)
}

func (a *FlowableActivity) DropFlowDestination(ctx context.Context, config *protos.ShutdownRequest) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return fmt.Errorf("failed to get destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	return dstConn.SyncFlowCleanup(ctx, config.FlowJobName)
}

func (a *FlowableActivity) SendWALHeartbeat(ctx context.Context) error {
	logger := activity.GetLogger(ctx)
	if !peerdbenv.PeerDBEnableWALHeartbeat() {
		logger.Info("wal heartbeat is disabled")
		return nil
	}

	pgPeers, err := a.getPostgresPeerConfigs(ctx)
	if err != nil {
		logger.Warn("[sendwalheartbeat] unable to fetch peers. " +
			"Skipping walheartbeat send. Error: " + err.Error())
		return err
	}

	command := `
		BEGIN;
		DROP AGGREGATE IF EXISTS PEERDB_EPHEMERAL_HEARTBEAT(float4);
		CREATE AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4) (SFUNC = float4pl, STYPE = float4);
		DROP AGGREGATE PEERDB_EPHEMERAL_HEARTBEAT(float4);
		END;
		`
	// run above command for each Postgres peer
	for _, pgPeer := range pgPeers {
		activity.RecordHeartbeat(ctx, pgPeer.Name)
		if ctx.Err() != nil {
			return nil
		}

		func() {
			pgConfig := pgPeer.GetPostgresConfig()
			peerConn, peerErr := pgx.Connect(ctx, shared.GetPGConnectionString(pgConfig))
			if peerErr != nil {
				logger.Error(fmt.Sprintf("error creating pool for postgres peer %v with host %v: %v",
					pgPeer.Name, pgConfig.Host, peerErr))
				return
			}
			defer peerConn.Close(ctx)

			_, err := peerConn.Exec(ctx, command)
			if err != nil {
				logger.Warn(fmt.Sprintf("could not send walheartbeat to peer %v: %v", pgPeer.Name, err))
			}

			logger.Info(fmt.Sprintf("sent walheartbeat to peer %v", pgPeer.Name))
		}()
	}

	return nil
}

func (a *FlowableActivity) RecordSlotSizes(ctx context.Context) error {
	rows, err := a.CatalogPool.Query(ctx, "SELECT DISTINCT ON (name) name, config_proto FROM flows WHERE query_string IS NULL")
	if err != nil {
		return err
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.FlowConnectionConfigs, error) {
		var flowName string
		var configProto []byte
		err := rows.Scan(&flowName, &configProto)
		if err != nil {
			return nil, err
		}

		var config protos.FlowConnectionConfigs
		err = proto.Unmarshal(configProto, &config)
		if err != nil {
			return nil, err
		}

		return &config, nil
	})
	if err != nil {
		return err
	}

	logger := activity.GetLogger(ctx)
	for _, config := range configs {
		func() {
			srcConn, err := connectors.GetCDCPullConnector(ctx, config.Source)
			if err != nil {
				if err != connectors.ErrUnsupportedFunctionality {
					logger.Error("Failed to create connector to handle slot info", slog.Any("error", err))
				}
				return
			}
			defer connectors.CloseConnector(ctx, srcConn)

			slotName := "peerflow_slot_" + config.FlowJobName
			if config.ReplicationSlotName != "" {
				slotName = config.ReplicationSlotName
			}
			peerName := config.Source.Name

			activity.RecordHeartbeat(ctx, fmt.Sprintf("checking %s on %s", slotName, peerName))
			if ctx.Err() != nil {
				return
			}

			var slotLagGauge *shared.Float64Gauge
			var openConnectionsGauge *shared.Int64Gauge
			if a.OtelManager != nil {
				// seriously
				var ok bool
				slotLagGaugeKey := fmt.Sprintf("%s_slotlag_%s", peerName, slotName)
				slotLagGauge, ok = a.OtelManager.SlotLagGaugesCache[slotLagGaugeKey]
				if !ok {
					slotLagGauge, err = shared.NewFloat64SyncGauge(a.OtelManager.SlotLagMeter,
						slotLagGaugeKey,
						metric.WithUnit("MB"),
						metric.WithDescription(fmt.Sprintf("Slot lag for slot %s on %s", slotName, peerName)))
					if err != nil {
						logger.Error("Failed to create slot lag gauge", slog.Any("error", err))
						return
					}
					a.OtelManager.SlotLagGaugesCache[slotLagGaugeKey] = slotLagGauge
				}

				openConnectionsGaugeKey := peerName + "_open_connections"
				openConnectionsGauge, ok = a.OtelManager.OpenConnectionsGaugesCache[openConnectionsGaugeKey]
				if !ok {
					openConnectionsGauge, err = shared.NewInt64SyncGauge(a.OtelManager.SlotLagMeter,
						openConnectionsGaugeKey,
						metric.WithUnit("connections"),
						metric.WithDescription("Current open connections for PeerDB user on "+peerName))
					if err != nil {
						logger.Error("Failed to create open connections gauge", slog.Any("error", err))
						return
					}
					a.OtelManager.OpenConnectionsGaugesCache[openConnectionsGaugeKey] = openConnectionsGauge
				}
			}

			err = srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, slotName, peerName,
				slotLagGauge, openConnectionsGauge)
			if err != nil {
				logger.Error("Failed to handle slot info", slog.Any("error", err))
			}
		}()
		if ctx.Err() != nil {
			return nil
		}
	}

	return nil
}

type QRepWaitUntilNewRowsResult struct {
	Found bool
}

func (a *FlowableActivity) QRepHasNewRows(ctx context.Context,
	config *protos.QRepConfig, last *protos.QRepPartition,
) (QRepWaitUntilNewRowsResult, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := log.With(activity.GetLogger(ctx), slog.String(string(shared.FlowNameKey), config.FlowJobName))

	if config.SourcePeer.Type != protos.DBType_POSTGRES || last.Range == nil {
		return QRepWaitUntilNewRowsResult{Found: true}, nil
	}

	logger.Info(fmt.Sprintf("current last partition value is %v", last))

	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return QRepWaitUntilNewRowsResult{Found: false}, fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "scanning for new rows"
	})
	defer shutdown()

	pgSrcConn := srcConn.(*connpostgres.PostgresConnector)
	result, err := pgSrcConn.CheckForUpdatedMaxValue(ctx, config, last)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return QRepWaitUntilNewRowsResult{Found: false}, fmt.Errorf("failed to check for new rows: %w", err)
	}

	return QRepWaitUntilNewRowsResult{Found: result}, nil
}

func (a *FlowableActivity) RenameTables(ctx context.Context, config *protos.RenameTablesInput) (
	*protos.RenameTablesOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	conn, err := connectors.GetAs[connectors.RenameTablesConnector](ctx, config.Peer)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, conn)

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "renaming tables for job"
	})
	defer shutdown()

	return conn.RenameTables(ctx, config)
}

func (a *FlowableActivity) CreateTablesFromExisting(ctx context.Context, req *protos.CreateTablesFromExistingInput) (
	*protos.CreateTablesFromExistingOutput, error,
) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, req.FlowJobName)
	dstConn, err := connectors.GetCDCSyncConnector(ctx, req.Peer)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	if req.Peer.Type == protos.DBType_SNOWFLAKE {
		sfConn, ok := dstConn.(*connsnowflake.SnowflakeConnector)
		if !ok {
			return nil, errors.New("failed to cast connector to snowflake connector")
		}
		return sfConn.CreateTablesFromExisting(ctx, req)
	} else if req.Peer.Type == protos.DBType_BIGQUERY {
		bqConn, ok := dstConn.(*connbigquery.BigQueryConnector)
		if !ok {
			return nil, errors.New("failed to cast connector to bigquery connector")
		}
		return bqConn.CreateTablesFromExisting(ctx, req)
	}
	a.Alerter.LogFlowError(ctx, req.FlowJobName, err)
	return nil, errors.New("create tables from existing is only supported on snowflake and bigquery")
}

// ReplicateXminPartition replicates a XminPartition from the source to the destination.
func (a *FlowableActivity) ReplicateXminPartition(ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	runUUID string,
) (int64, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	startTime := time.Now()
	srcConn, err := connectors.GetQRepPullConnector(ctx, config.SourcePeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	dstConn, err := connectors.GetQRepSyncConnector(ctx, config.DestinationPeer)
	if err != nil {
		return 0, fmt.Errorf("failed to get qrep destination connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, dstConn)

	logger.Info("replicating xmin")

	bufferSize := shared.FetchAndChannelSize
	errGroup, errCtx := errgroup.WithContext(ctx)

	stream := model.NewQRecordStream(bufferSize)

	var currentSnapshotXmin int64
	errGroup.Go(func() error {
		pgConn := srcConn.(*connpostgres.PostgresConnector)
		var pullErr error
		var numRecords int
		numRecords, currentSnapshotXmin, pullErr = pgConn.PullXminRecordStream(ctx, config, partition, stream)
		if pullErr != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			logger.Warn(fmt.Sprintf("[xmin] failed to pull records: %v", err))
			return err
		}

		// The first sync of an XMIN mirror will have a partition without a range
		// A nil range is not supported by the catalog mirror monitor functions below
		// So I'm creating a partition with a range of 0 to numRecords
		partitionForMetrics := partition
		if partition.Range == nil {
			partitionForMetrics = &protos.QRepPartition{
				PartitionId: partition.PartitionId,
				Range: &protos.PartitionRange{
					Range: &protos.PartitionRange_IntRange{
						IntRange: &protos.IntPartitionRange{Start: 0, End: int64(numRecords)},
					},
				},
			}
		}
		updateErr := monitoring.InitializeQRepRun(
			ctx, a.CatalogPool, config, runUUID, []*protos.QRepPartition{partitionForMetrics})
		if updateErr != nil {
			return updateErr
		}

		err := monitoring.UpdateStartTimeForPartition(ctx, a.CatalogPool, runUUID, partition, startTime)
		if err != nil {
			return fmt.Errorf("failed to update start time for partition: %w", err)
		}

		err = monitoring.UpdatePullEndTimeAndRowsForPartition(
			errCtx, a.CatalogPool, runUUID, partition, int64(numRecords))
		if err != nil {
			logger.Error(err.Error())
			return err
		}

		return nil
	})

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return "syncing xmin."
	})
	defer shutdown()

	rowsSynced, err := dstConn.SyncQRepRecords(ctx, config, partition, stream)
	if err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return 0, fmt.Errorf("failed to sync records: %w", err)
	}

	if rowsSynced == 0 {
		logger.Info("no records to push for xmin")
	} else {
		err := errGroup.Wait()
		if err != nil {
			a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
			return 0, err
		}

		err = monitoring.UpdateRowsSyncedForPartition(ctx, a.CatalogPool, rowsSynced, runUUID, partition)
		if err != nil {
			return 0, err
		}

		logger.Info(fmt.Sprintf("pushed %d records", rowsSynced))
	}

	err = monitoring.UpdateEndTimeForPartition(ctx, a.CatalogPool, runUUID, partition)
	if err != nil {
		return 0, err
	}

	return currentSnapshotXmin, nil
}

func (a *FlowableActivity) AddTablesToPublication(ctx context.Context, cfg *protos.FlowConnectionConfigs,
	additionalTableMappings []*protos.TableMapping,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, cfg.FlowJobName)
	srcConn, err := connectors.GetCDCPullConnector(ctx, cfg.Source)
	if err != nil {
		return fmt.Errorf("failed to get source connector: %w", err)
	}
	defer connectors.CloseConnector(ctx, srcConn)

	err = srcConn.AddTablesToPublication(ctx, &protos.AddTablesToPublicationInput{
		FlowJobName:      cfg.FlowJobName,
		PublicationName:  cfg.PublicationName,
		AdditionalTables: additionalTableMappings,
	})
	if err != nil {
		a.Alerter.LogFlowError(ctx, cfg.FlowJobName, err)
	}
	return err
}
