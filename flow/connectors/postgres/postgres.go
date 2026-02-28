package connpostgres

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	numeric "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type ReplState struct {
	Slot        string
	Publication string
	Offset      int64
	LastOffset  atomic.Int64
}

type PostgresConnector struct {
	logger                 log.Logger
	customTypeMapping      map[uint32]shared.CustomDataType
	ssh                    *utils.SSHTunnel
	conn                   *pgx.Conn
	replConn               *pgx.Conn
	replState              *ReplState
	Config                 *protos.PostgresConfig
	hushWarnOID            map[uint32]struct{}
	relationMessageMapping model.RelationMessageMapping
	typeMap                *pgtype.Map
	rdsAuth                *utils.RDSAuth
	connStr                string
	metadataSchema         string
	replLock               sync.Mutex
	pgVersion              shared.PGVersion
}

func NewPostgresConnector(ctx context.Context, env map[string]string, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	logger := internal.LoggerFromCtx(ctx)
	flowNameInApplicationName, err := internal.PeerDBApplicationNamePerMirrorName(ctx, nil)
	if err != nil {
		logger.Error("Failed to get flow name from application name", slog.Any("error", err))
	}
	var flowName string
	if flowNameInApplicationName {
		flowName, _ = ctx.Value(shared.FlowNameKey).(string)
	}
	connectionString := internal.GetPGConnectionString(pgConfig, flowName)
	connConfig, err := ParseConfig(connectionString, pgConfig)
	if err != nil {
		return nil, err
	}

	connConfig.Config.RuntimeParams["timezone"] = "UTC"
	connConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	connConfig.Config.RuntimeParams["statement_timeout"] = "0"
	connConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"

	tunnel, err := utils.NewSSHTunnel(ctx, pgConfig.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	var rdsAuth *utils.RDSAuth
	if pgConfig.AuthType == protos.PostgresAuthType_POSTGRES_IAM_AUTH {
		rdsAuth = &utils.RDSAuth{
			AwsAuthConfig: pgConfig.AwsAuth,
		}
		if err := rdsAuth.VerifyAuthConfig(); err != nil {
			logger.Error("failed to verify auth config", slog.Any("error", err))
			return nil, fmt.Errorf("failed to verify auth config: %w", err)
		}
	}
	conn, err := NewPostgresConnFromConfig(ctx, connConfig, pgConfig.TlsHost, rdsAuth, tunnel)
	if err != nil {
		tunnel.Close()
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if pgConfig.MetadataSchema != nil {
		metadataSchema = *pgConfig.MetadataSchema
	}

	return &PostgresConnector{
		logger:                 logger,
		Config:                 pgConfig,
		ssh:                    tunnel,
		conn:                   conn,
		replConn:               nil,
		replState:              nil,
		customTypeMapping:      nil,
		hushWarnOID:            make(map[uint32]struct{}),
		relationMessageMapping: make(model.RelationMessageMapping),
		connStr:                connectionString,
		metadataSchema:         metadataSchema,
		replLock:               sync.Mutex{},
		pgVersion:              0,
		typeMap:                pgtype.NewMap(),
		rdsAuth:                rdsAuth,
	}, nil
}

func ParseConfig(connectionString string, pgConfig *protos.PostgresConfig) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if pgConfig.RequireTls || pgConfig.RootCa != nil {
		tlsConfig, err := common.CreateTlsConfig(tls.VersionTLS12, pgConfig.RootCa, connConfig.Host, pgConfig.TlsHost, false)
		if err != nil {
			return nil, err
		}
		connConfig.TLSConfig = tlsConfig
	}
	return connConfig, nil
}

func (c *PostgresConnector) fetchCustomTypeMapping(ctx context.Context) (map[uint32]shared.CustomDataType, error) {
	if c.customTypeMapping == nil {
		customTypeMapping, err := shared.GetCustomDataTypes(ctx, c.conn)
		if err != nil {
			return nil, err
		}
		c.customTypeMapping = customTypeMapping
	}
	return c.customTypeMapping, nil
}

func (c *PostgresConnector) CreateReplConn(ctx context.Context, env map[string]string) (*pgx.Conn, error) {
	// create a separate connection for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	replConfig, err := ParseConfig(c.connStr, c.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	replConfig.Config.RuntimeParams["timezone"] = "UTC"
	replConfig.Config.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
	replConfig.Config.RuntimeParams["statement_timeout"] = "0"
	replConfig.Config.RuntimeParams["replication"] = "database"
	replConfig.Config.RuntimeParams["bytea_output"] = "hex"
	replConfig.Config.RuntimeParams["intervalstyle"] = "postgres"
	replConfig.Config.RuntimeParams["DateStyle"] = "ISO, DMY"
	replConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	walSenderTimeout, err := internal.PeerDBPostgresWalSenderTimeout(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get wal_sender_timeout value: %w", err)
	}
	if !strings.EqualFold(walSenderTimeout, "NONE") {
		c.logger.Info("set wal_sender_timeout", slog.String("wal_sender_timeout", walSenderTimeout))
		replConfig.Config.RuntimeParams["wal_sender_timeout"] = walSenderTimeout
	} else {
		c.logger.Info("not setting wal_sender_timeout")
	}

	conn, err := NewPostgresConnFromConfig(ctx, replConfig, c.Config.TlsHost, c.rdsAuth, c.ssh)
	if err != nil {
		internal.LoggerFromCtx(ctx).Error("failed to create replication connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create replication connection: %w", err)
	}
	return conn, nil
}

func (c *PostgresConnector) SetupReplConn(ctx context.Context, env map[string]string) error {
	conn, err := c.CreateReplConn(ctx, env)
	if err != nil {
		return err
	}
	c.replConn = conn
	return nil
}

// To keep connection alive between sync batches.
// By default postgres drops connection after 1 minute of inactivity.
func (c *PostgresConnector) ReplPing(ctx context.Context) error {
	if c.replLock.TryLock() {
		defer c.replLock.Unlock()
		if c.replState != nil {
			return pglogrepl.SendStandbyStatusUpdate(
				ctx,
				c.replConn.PgConn(),
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(c.replState.LastOffset.Load())},
			)
		}
	}
	return nil
}

func (c *PostgresConnector) MaybeStartReplication(
	ctx context.Context,
	slotName string,
	publicationName string,
	lastOffset int64,
	pgVersion shared.PGVersion,
) error {
	if c.replState != nil && (c.replState.Offset != lastOffset ||
		c.replState.Slot != slotName ||
		c.replState.Publication != publicationName) {
		msg := fmt.Sprintf("replState changed, reset connector. slot name: old=%s new=%s, publication: old=%s new=%s, offset: old=%d new=%d",
			c.replState.Slot, slotName, c.replState.Publication, publicationName, c.replState.Offset, lastOffset,
		)
		c.logger.Info(msg)
		return exceptions.NewReplStateDesyncError(msg)
	}

	if c.replState == nil {
		replicationOpts, err := c.replicationOptions(publicationName, pgVersion)
		if err != nil {
			return fmt.Errorf("error getting replication options: %w", err)
		}

		var startLSN pglogrepl.LSN
		if lastOffset > 0 {
			c.logger.Info("starting replication from last sync state", slog.Int64("last checkpoint", lastOffset))
			startLSN = pglogrepl.LSN(lastOffset + 1)
		}

		c.replLock.Lock()
		defer c.replLock.Unlock()
		if err := pglogrepl.StartReplication(
			ctx, c.replConn.PgConn(), common.QuoteIdentifier(slotName), startLSN, replicationOpts); err != nil {
			c.logger.Error("error starting replication", slog.Any("error", err))
			return fmt.Errorf("error starting replication at startLsn - %d: %w", startLSN, err)
		}

		c.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", slotName, startLSN))
		c.replState = &ReplState{
			Slot:        slotName,
			Publication: publicationName,
			Offset:      lastOffset,
			LastOffset:  atomic.Int64{},
		}
		c.replState.LastOffset.Store(lastOffset)
	}
	return nil
}

func (c *PostgresConnector) replicationOptions(publicationName string, pgVersion shared.PGVersion,
) (pglogrepl.StartReplicationOptions, error) {
	pluginArguments := append(make([]string, 0, 3), "proto_version '1'")

	if publicationName != "" {
		pubOpt := "publication_names " + utils.QuoteLiteral(publicationName)
		pluginArguments = append(pluginArguments, pubOpt)
	} else {
		return pglogrepl.StartReplicationOptions{}, errors.New("publication name is not set")
	}

	if pgVersion >= shared.POSTGRES_14 {
		pluginArguments = append(pluginArguments, "messages 'true'")
	}

	return pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	var errs []error
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.conn.Close(timeout); err != nil {
			c.logger.Error("failed to close Postgres connection", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("failed to close Postgres connection: %w", err))
		}

		if c.replConn != nil {
			timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.replConn.Close(timeout); err != nil {
				c.logger.Error("failed to close Postgres replication connection", slog.Any("error", err))
				errs = append(errs, fmt.Errorf("failed to close Postgres replication connection: %w", err))
			}
		}

		if err := c.ssh.Close(); err != nil {
			c.logger.Error("[postgres] failed to close SSH tunnel", slog.Any("error", err))
			errs = append(errs, fmt.Errorf("[postgres] failed to close SSH tunnel: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (c *PostgresConnector) Conn() *pgx.Conn {
	return c.conn
}

// ConnectionActive returns nil if the connection is active.
func (c *PostgresConnector) ConnectionActive(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("connection is nil")
	}
	_, pingErr := c.conn.Exec(ctx, "SELECT 1")
	return pingErr
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables(ctx context.Context) (bool, error) {
	result, err := c.tableExists(ctx, &common.QualifiedTable{
		Namespace: c.metadataSchema,
		Table:     mirrorJobsTableIdentifier,
	})
	return !result, err
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables(ctx context.Context) error {
	if err := c.createMetadataSchema(ctx); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx,
		fmt.Sprintf(createMirrorJobsTableSQL, c.metadataSchema, mirrorJobsTableIdentifier),
	); err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation, pgerrcode.DuplicateObject) {
		return fmt.Errorf("error creating table %s: %w", mirrorJobsTableIdentifier, err)
	}

	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(ctx context.Context, jobName string) (model.CdcCheckpoint, error) {
	var result model.CdcCheckpoint
	if err := c.conn.QueryRow(
		ctx, fmt.Sprintf(getLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName,
	).Scan(&result.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Info("No row found, returning nil")
			return result, nil
		}
		return result, fmt.Errorf("error while reading result row: %w", err)
	}

	if result.ID == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
	}
	return result, nil
}

// SetLastOffset updates the last synced offset for a job.
func (c *PostgresConnector) SetLastOffset(ctx context.Context, jobName string, lastOffset model.CdcCheckpoint) error {
	if _, err := c.conn.Exec(ctx,
		fmt.Sprintf(setLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		lastOffset.ID, jobName,
	); err != nil {
		return fmt.Errorf("error setting last offset for job %s: %w", jobName, err)
	}

	return nil
}

func (c *PostgresConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	return pullCore(ctx, c, catalogPool, otelManager, req, qProcessor{})
}

func (c *PostgresConnector) PullPg(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.PgItems],
) error {
	return pullCore(ctx, c, catalogPool, otelManager, req, pgProcessor{})
}

// PullRecords pulls records from the source.
func pullCore[Items model.Items](
	ctx context.Context,
	c *PostgresConnector,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
) error {
	defer func() {
		req.RecordStream.Close()
		if c.replState != nil {
			c.replState.Offset = req.RecordStream.GetLastCheckpoint().ID
		}
	}()

	slotName := GetDefaultSlotName(req.FlowJobName)
	if req.OverrideReplicationSlotName != "" {
		slotName = req.OverrideReplicationSlotName
	}

	publicationName := GetDefaultPublicationName(req.FlowJobName)
	if req.OverridePublicationName != "" {
		publicationName = req.OverridePublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
	}

	if !exists.PublicationExists {
		c.logger.Warn("publication does not exist", slog.String("name", publicationName))
		return exceptions.NewPublicationMissingError(publicationName)
	}

	if !exists.SlotExists {
		c.logger.Warn("slot does not exist", slog.String("name", slotName))
		return exceptions.NewSlotMissingError(slotName)
	}

	c.logger.Info("PullRecords: performed checks for slot and publication")

	// cached, since this connector is reused
	pgVersion, err := c.MajorVersion(ctx)
	if err != nil {
		return err
	}
	if err := c.MaybeStartReplication(ctx, slotName, publicationName, req.LastOffset.ID, pgVersion); err != nil {
		c.logger.Error("error starting replication", slog.Any("error", err))
		return err
	}
	handleInheritanceForNonPartitionedTables, err := internal.PeerDBPostgresCDCHandleInheritanceForNonPartitionedTables(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for handleInheritanceForNonPartitionedTables: %w", err)
	}
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for sourceSchemaAsDestinationColumn: %w", err)
	}
	originMetaAsDestinationColumn, err := internal.PeerDBOriginMetaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return fmt.Errorf("failed to get get setting for originMetaAsDestinationColumn: %w", err)
	}

	cdc, err := c.NewPostgresCDCSource(ctx, &PostgresCDCConfig{
		CatalogPool:                              catalogPool,
		OtelManager:                              otelManager,
		SrcTableIDNameMapping:                    req.SrcTableIDNameMapping,
		TableNameMapping:                         req.TableNameMapping,
		TableNameSchemaMapping:                   req.TableNameSchemaMapping,
		RelationMessageMapping:                   c.relationMessageMapping,
		FlowJobName:                              req.FlowJobName,
		Slot:                                     slotName,
		Publication:                              publicationName,
		HandleInheritanceForNonPartitionedTables: handleInheritanceForNonPartitionedTables,
		SourceSchemaAsDestinationColumn:          sourceSchemaAsDestinationColumn,
		OriginMetaAsDestinationColumn:            originMetaAsDestinationColumn,
		InternalVersion:                          req.InternalVersion,
	})
	if err != nil {
		c.logger.Error("error creating cdc source", slog.Any("error", err))
		return err
	}

	if err := PullCdcRecords(ctx, cdc, req, processor, &c.replLock); err != nil {
		c.logger.Error("error pulling records", slog.Any("error", err))
		return err
	}

	// Since this is just a monitoring metric, we can ignore errors about LSN
	if latestLSN, err := c.getCurrentLSN(ctx); err != nil {
		c.logger.Error("error getting current LSN", slog.Any("error", err))
	} else if latestLSN.Null {
		c.logger.Info("Current LSN null, probably read replica starting up")
	} else if err := monitoring.UpdateLatestLSNAtSourceForCDCFlow(ctx, catalogPool, req.FlowJobName, int64(latestLSN.LSN)); err != nil {
		c.logger.Error("error updating latest LSN at source for CDC flow", slog.Any("error", err))
	}

	return nil
}

func (c *PostgresConnector) UpdateReplStateLastOffset(_ context.Context, lastOffset model.CdcCheckpoint) error {
	if c.replState != nil {
		c.replState.LastOffset.Store(lastOffset.ID)
	}
	return nil
}

func (c *PostgresConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	return syncRecordsCore(ctx, c, req)
}

func (c *PostgresConnector) SyncPg(ctx context.Context, req *model.SyncRecordsRequest[model.PgItems]) (*model.SyncResponse, error) {
	return syncRecordsCore(ctx, c, req)
}

// syncRecordsCore pushes records to the destination.
func syncRecordsCore[Items model.Items](
	ctx context.Context,
	c *PostgresConnector,
	req *model.SyncRecordsRequest[Items],
) (*model.SyncResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	c.logger.Info(fmt.Sprintf("pushing records to Postgres table %s via COPY", rawTableIdentifier))

	numRecords := int64(0)
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)
	streamReadFunc := func() ([]any, error) {
		for record := range req.Records.GetRecords() {
			var row []any
			switch typedRecord := record.(type) {
			case *model.InsertRecord[Items]:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize insert record items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					0,
					"{}",
					req.SyncBatchID,
					"",
				}

			case *model.UpdateRecord[Items]:
				newItemsJSON, err := typedRecord.NewItems.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record new items to JSON: %w", err)
				}
				oldItemsJSON, err := typedRecord.OldItems.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize update record old items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					newItemsJSON,
					1,
					oldItemsJSON,
					req.SyncBatchID,
					utils.KeysToString(typedRecord.UnchangedToastColumns),
				}

			case *model.DeleteRecord[Items]:
				itemsJSON, err := typedRecord.Items.ToJSONWithOptions(model.ToJSONOptions{
					UnnestColumns: nil,
					HStoreAsJSON:  false,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to serialize delete record items to JSON: %w", err)
				}

				row = []any{
					uuid.New(),
					time.Now().UnixNano(),
					typedRecord.DestinationTableName,
					itemsJSON,
					2,
					itemsJSON,
					req.SyncBatchID,
					"",
				}

			case *model.MessageRecord[Items]:
				continue

			default:
				return nil, fmt.Errorf("unsupported record type for Postgres flow connector: %T", typedRecord)
			}

			record.PopulateCountMap(tableNameRowsMapping)
			numRecords += 1
			return row, nil
		}

		return nil, nil
	}

	syncRecordsTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for syncing records: %w", err)
	}
	defer shared.RollbackTx(syncRecordsTx, c.logger)

	syncedRecordsCount, err := syncRecordsTx.CopyFrom(ctx, pgx.Identifier{c.metadataSchema, rawTableIdentifier},
		[]string{
			"_peerdb_uid", "_peerdb_timestamp", "_peerdb_destination_table_name", "_peerdb_data",
			"_peerdb_record_type", "_peerdb_match_data", "_peerdb_batch_id", "_peerdb_unchanged_toast_columns",
		},
		pgx.CopyFromFunc(streamReadFunc))
	if err != nil {
		return nil, fmt.Errorf("error syncing records: %w", err)
	}
	if syncedRecordsCount != numRecords {
		return nil, fmt.Errorf("error syncing records: expected %d records to be synced, but %d were synced",
			numRecords, syncedRecordsCount)
	}

	c.logger.Info(fmt.Sprintf("synced %d records to Postgres table %s via COPY",
		syncedRecordsCount, rawTableIdentifier))

	// updating metadata with new offset and syncBatchID
	lastCP := req.Records.GetLastCheckpoint()
	if err := c.updateSyncMetadata(ctx, req.FlowJobName, lastCP, req.SyncBatchID, syncRecordsTx); err != nil {
		return nil, err
	}
	// transaction commits
	if err := syncRecordsTx.Commit(ctx); err != nil {
		return nil, err
	}

	if err := c.ReplayTableSchemaDeltas(ctx, req.Env, req.FlowJobName, req.TableMappings, req.Records.SchemaDeltas, nil); err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpoint: lastCP,
		NumRecordsSynced:     numRecords,
		CurrentSyncBatchID:   req.SyncBatchID,
		TableNameRowsMapping: tableNameRowsMapping,
		TableSchemaDeltas:    req.Records.SchemaDeltas,
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(
	ctx context.Context,
	req *model.NormalizeRecordsRequest,
) (model.NormalizeResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	jobMetadataExists, err := c.jobMetadataExists(ctx, req.FlowJobName)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	// no SyncFlow has run, chill until more records are loaded.
	if !jobMetadataExists {
		c.logger.Info("no metadata found for mirror")
		return model.NormalizeResponse{}, nil
	}

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return model.NormalizeResponse{}, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		c.logger.Info(fmt.Sprintf("no records to normalize: syncBatchID %d, normalizeBatchID %d",
			req.SyncBatchID, normBatchID))
		return model.NormalizeResponse{
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx, req.FlowJobName, req.SyncBatchID, normBatchID, req.TableNameSchemaMapping)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	unchangedToastColumnsMap, err := c.getTableNametoUnchangedCols(ctx, req.FlowJobName,
		req.SyncBatchID, normBatchID)
	if err != nil {
		return model.NormalizeResponse{}, err
	}

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return model.NormalizeResponse{}, err
	}
	totalRowsAffected := 0
	normalizeStmtGen := normalizeStmtGenerator{
		Logger:                   c.logger,
		rawTableName:             rawTableIdentifier,
		tableSchemaMapping:       req.TableNameSchemaMapping,
		unchangedToastColumnsMap: unchangedToastColumnsMap,
		peerdbCols: &protos.PeerDBColumns{
			SoftDeleteColName: req.SoftDeleteColName,
			SyncedAtColName:   req.SyncedAtColName,
		},
		supportsMerge:  pgversion >= shared.POSTGRES_15,
		metadataSchema: c.metadataSchema,
	}

	for batchID := normBatchID + 1; batchID <= req.SyncBatchID; batchID++ {
		rowsAffected, err := c.normalizeBatch(ctx, batchID, req, destinationTableNames, &normalizeStmtGen)
		if err != nil {
			return model.NormalizeResponse{}, err
		}
		totalRowsAffected += rowsAffected
		c.logger.Info("normalize: committed batch to destination",
			slog.Int64("batchID", batchID),
			slog.Int64("syncBatchID", req.SyncBatchID),
			slog.Int("rowsAffected", rowsAffected),
		)
	}
	c.logger.Info(fmt.Sprintf("normalized %d records", totalRowsAffected))

	return model.NormalizeResponse{
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
}

type batchEntry struct {
	tableName string
	statement string
}

func (c *PostgresConnector) normalizeBatch(
	ctx context.Context,
	batchID int64,
	req *model.NormalizeRecordsRequest,
	destinationTableNames []string,
	normalizeStmtGen *normalizeStmtGenerator,
) (int, error) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("error starting transaction for normalizing records: %w", err)
	}
	defer shared.RollbackTx(tx, c.logger)

	batch := &pgx.Batch{}
	var entries []batchEntry
	for _, destinationTableName := range destinationTableNames {
		normalizeStatements := normalizeStmtGen.generateNormalizeStatements(destinationTableName)
		for _, stmt := range normalizeStatements {
			batch.Queue(stmt, batchID, destinationTableName)
			entries = append(entries, batchEntry{tableName: destinationTableName, statement: stmt})
		}
	}
	batch.Queue(
		fmt.Sprintf(updateMetadataForNormalizeRecordsSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		batchID, req.FlowJobName,
	)

	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	totalRowsAffected := 0
	for _, entry := range entries {
		ct, err := results.Exec()
		if err != nil {
			c.logger.Error("error executing normalize statement",
				slog.String("statement", entry.statement),
				slog.Int64("batchID", batchID),
				slog.String("destinationTableName", entry.tableName),
				slog.Any("error", err),
			)
			return 0, fmt.Errorf("error executing normalize statement for table %s: %w", entry.tableName, err)
		}
		totalRowsAffected += int(ct.RowsAffected())
	}

	if _, err := results.Exec(); err != nil {
		return 0, fmt.Errorf("failed to update metadata for NormalizeTables: %w", err)
	}

	if err := results.Close(); err != nil {
		return 0, fmt.Errorf("failed to close batch results: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit normalize transaction: %w", err)
	}

	return totalRowsAffected, nil
}

type SlotCheckResult struct {
	SlotExists        bool
	PublicationExists bool
}

// CreateRawTable creates a raw table, implementing the Connector interface.
func (c *PostgresConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	err := c.createMetadataSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating internal schema: %w", err)
	}

	createRawTableTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for creating raw table: %w", err)
	}
	defer shared.RollbackTx(createRawTableTx, c.logger)

	if _, err := createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier)); err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}
	if _, err := createRawTableTx.Exec(ctx,
		fmt.Sprintf(createRawTableBatchIDIndexSQL, rawTableIdentifier, c.metadataSchema, rawTableIdentifier),
	); err != nil {
		return nil, fmt.Errorf("error creating batch ID index on raw table: %w", err)
	}
	if _, err := createRawTableTx.Exec(ctx,
		fmt.Sprintf(createRawTableDstTableIndexSQL, rawTableIdentifier, c.metadataSchema, rawTableIdentifier),
	); err != nil {
		return nil, fmt.Errorf("error creating destination table index on raw table: %w", err)
	}

	if err := createRawTableTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}

	return nil, nil
}

func (c *PostgresConnector) StatActivity(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	rows, err := c.Conn().Query(ctx, "SELECT pid, wait_event, wait_event_type, query_start::text, query,"+
		"CAST(EXTRACT(epoch FROM(now()-query_start)) AS float4) AS dur, state"+
		" FROM pg_stat_activity WHERE "+
		"usename=$1 AND application_name LIKE 'peerdb%';", c.Config.User)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get stat info", slog.Any("error", err))
		return nil, err
	}

	statInfoRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.StatInfo, error) {
		var pid int64
		var waitEvent pgtype.Text
		var waitEventType pgtype.Text
		var queryStart pgtype.Text
		var query pgtype.Text
		var duration pgtype.Float4
		// shouldn't be null
		var state string

		if err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration, &state); err != nil {
			slog.ErrorContext(ctx, "Failed to scan row", slog.Any("error", err))
			return nil, err
		}

		we := waitEvent.String
		if !waitEvent.Valid {
			we = ""
		}

		wet := waitEventType.String
		if !waitEventType.Valid {
			wet = ""
		}

		q := query.String
		if !query.Valid {
			q = ""
		}

		qs := queryStart.String
		if !queryStart.Valid {
			qs = ""
		}

		d := duration.Float32
		if !duration.Valid {
			d = -1
		}

		return &protos.StatInfo{
			Pid:           pid,
			WaitEvent:     we,
			WaitEventType: wet,
			QueryStart:    qs,
			Query:         q,
			Duration:      d,
			State:         state,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}

func (c *PostgresConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMapping []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMapping))
	typeSchemaNameMapping := make(map[uint32]string, len(tableMapping))
	for _, tm := range tableMapping {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system, version, typeSchemaNameMapping)
		if err != nil {
			c.logger.Info("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}

	return res, nil
}

func (c *PostgresConnector) GetSelectedColumns(
	ctx context.Context,
	sourceTable *common.QualifiedTable,
	excludedColumns []string,
) ([]string, error) {
	quotedExcludedColumns := make([]string, 0, len(excludedColumns))
	for _, col := range excludedColumns {
		quotedExcludedColumns = append(quotedExcludedColumns, utils.QuoteLiteral(col))
	}
	excludedColumnsSQL := ""
	if len(excludedColumns) > 0 {
		excludedColumnsSQL = "AND a.attname NOT IN (" + strings.Join(quotedExcludedColumns, ",") + ")"
	}

	getColumnsSQL := `
		SELECT a.attname AS column_name
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped ` + excludedColumnsSQL

	rows, err := c.conn.Query(ctx, getColumnsSQL, sourceTable.Namespace, sourceTable.Table)
	if err != nil {
		c.logger.Error("error getting selected columns",
			slog.Any("error", err),
			slog.String("table", sourceTable.String()),
			slog.Any("excludedColumns", excludedColumns))
		return nil, fmt.Errorf("error getting selected columns for table %s: %w", sourceTable, err)
	}

	columns := make([]string, 0)
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning column while getting selected columns: %w", err)
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

func (c *PostgresConnector) GetTablesFromPublication(
	ctx context.Context,
	publicationName string,
	excludedTables []*common.QualifiedTable,
) ([]*common.QualifiedTable, error) {
	var getTablesSQL string
	var args []any

	if len(excludedTables) == 0 {
		// No filter - get all tables from publication
		getTablesSQL = `SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY schemaname, tablename`
		args = []any{publicationName}
	} else {
		// Get tables that are in publication but NOT in the filter list
		schemas := make([]string, len(excludedTables))
		tables := make([]string, len(excludedTables))
		for i, schemaTable := range excludedTables {
			schemas[i] = schemaTable.Namespace
			tables[i] = schemaTable.Table
		}
		getTablesSQL = `
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = $1
            AND (schemaname, tablename) NOT IN (
                SELECT a.val AS schemaname, b.val AS tablename
                FROM unnest($2::text[]) WITH ORDINALITY AS a(val, idx)
                FULL JOIN unnest($3::text[]) WITH ORDINALITY AS b(val, idx)
                USING (idx)
            )
            ORDER BY schemaname, tablename`
		args = []any{publicationName, schemas, tables}
	}

	rows, err := c.conn.Query(ctx, getTablesSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("error getting tables from publication %s: %w", publicationName, err)
	}

	tables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*common.QualifiedTable, error) {
		var schemaName, tableName string
		if err := row.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning row while getting tables from publication %s: %w", publicationName, err)
		}
		return &common.QualifiedTable{
			Namespace: schemaName,
			Table:     tableName,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error collecting rows while getting tables from publication %s: %w", publicationName, err)
	}

	return tables, nil
}

/*
GetSchemaNameOfColumnTypeByOID returns a map of type OID to schema name for the given OIDs.
If the type is in the "pg_catalog" schema, it returns an empty string for that OID.
*/
func (c *PostgresConnector) GetSchemaNameOfColumnTypeByOID(ctx context.Context, typeOIDs []uint32) (map[uint32]string, error) {
	if len(typeOIDs) == 0 {
		return make(map[uint32]string), nil
	}

	rows, err := c.conn.Query(ctx, `
		SELECT t.oid, n.nspname
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		WHERE t.oid = ANY($1)
	`, typeOIDs)
	if err != nil {
		return nil, fmt.Errorf("error getting schema of column types: %w", err)
	}

	result := make(map[uint32]string, len(typeOIDs))
	var oid uint32
	var schemaName string
	if _, err := pgx.ForEachRow(rows, []any{&oid, &schemaName}, func() error {
		result[oid] = schemaName
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error scanning rows for schema of column types: %w", err)
	}

	return result, nil
}

func (c *PostgresConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tm *protos.TableMapping,
	system protos.TypeSystem,
	version uint32,
	typeSchemaNameMapping map[uint32]string,
) (*protos.TableSchema, error) {
	schemaTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}
	customTypeMapping, err := c.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	relID, err := c.getRelIDForTable(ctx, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] failed to get relation id for table %s: %w", schemaTable, err)
	}

	replicaIdentityType, err := c.getReplicaIdentityType(ctx, relID, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting replica identity for table %s: %w", schemaTable, err)
	}

	pKeyCols, err := c.getUniqueColumns(ctx, relID, replicaIdentityType, schemaTable)
	if err != nil {
		return nil, fmt.Errorf("[getTableSchema] error getting primary key column for table %s: %w", schemaTable, err)
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	var nullableCols map[string]struct{}
	nullableCols, err = c.getNullableColumns(ctx, relID)
	if err != nil {
		return nil, err
	}

	selectedColumnsStr := "*"
	if len(tm.Exclude) > 0 {
		selectedColumns, err := c.GetSelectedColumns(ctx, schemaTable, tm.Exclude)
		if err != nil {
			return nil, err
		}

		if len(selectedColumns) == 0 {
			return nil, fmt.Errorf("no columns selected for table %s", schemaTable)
		}

		for i, col := range selectedColumns {
			selectedColumns[i] = common.QuoteIdentifier(col)
		}
		selectedColumnsStr = strings.Join(selectedColumns, ",")
	}

	// Get the column names and types
	rows, err := c.conn.Query(ctx,
		fmt.Sprintf(`SELECT %s FROM %s LIMIT 0`, selectedColumnsStr, schemaTable.String()),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}

	// Make a copy of field descriptions since pgx may reuse the underlying array
	fields := slices.Clone(rows.FieldDescriptions())
	rows.Close() // Close rows before making another query
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error after fetching field descriptions for table %s: %w", schemaTable, err)
	}

	columnNames := make([]string, 0, len(fields))
	columns := make([]*protos.FieldDescription, 0, len(fields))
	// Collect OIDs that we haven't fetched yet (deduplicate using a map)
	unfetchedOIDsMap := make(map[uint32]struct{})
	for _, fieldDescription := range fields {
		if _, exists := typeSchemaNameMapping[fieldDescription.DataTypeOID]; !exists {
			unfetchedOIDsMap[fieldDescription.DataTypeOID] = struct{}{}
		}
	}

	unfetchedOIDs := slices.Collect(maps.Keys(unfetchedOIDsMap))

	// Fetch schema names for unfetched OIDs and add to shared map
	if len(unfetchedOIDs) > 0 {
		newTypeSchemaNames, err := c.GetSchemaNameOfColumnTypeByOID(ctx, unfetchedOIDs)
		if err != nil {
			return nil, fmt.Errorf("error getting schema names for column types: %w", err)
		}
		maps.Copy(typeSchemaNameMapping, newTypeSchemaNames)
	}

	for _, fieldDescription := range fields {
		var colType string
		var err error
		switch system {
		case protos.TypeSystem_PG:
			colType, err = c.postgresOIDToName(fieldDescription.DataTypeOID, customTypeMapping)
		case protos.TypeSystem_Q:
			qColType := c.postgresOIDToQValueKind(fieldDescription.DataTypeOID, customTypeMapping, version)
			colType = string(qColType)
		}
		if err != nil {
			return nil, fmt.Errorf("error getting type name for %d: %w", fieldDescription.DataTypeOID, err)
		}

		columnNames = append(columnNames, fieldDescription.Name)
		_, nullable := nullableCols[fieldDescription.Name]
		columns = append(columns, &protos.FieldDescription{
			Name:           fieldDescription.Name,
			Type:           colType,
			TypeModifier:   fieldDescription.TypeModifier,
			Nullable:       nullable,
			TypeSchemaName: typeSchemaNameMapping[fieldDescription.DataTypeOID],
		})
	}

	// if we have no pkey, we will use all columns as the pkey for the MERGE statement
	if replicaIdentityType == ReplicaIdentityFull && len(pKeyCols) == 0 {
		pKeyCols = columnNames
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: replicaIdentityType == ReplicaIdentityFull,
		Columns:               columns,
		NullableEnabled:       nullableEnabled,
		System:                system,
		TableOid:              relID,
	}, nil
}

func (c *PostgresConnector) StartSetupNormalizedTables(ctx context.Context) (any, error) {
	// Postgres is cool and supports transactional DDL. So we use a transaction.
	return c.conn.Begin(ctx)
}

func (c *PostgresConnector) CleanupSetupNormalizedTables(ctx context.Context, tx any) {
	shared.RollbackTx(tx.(pgx.Tx), c.logger)
}

func (c *PostgresConnector) FinishSetupNormalizedTables(ctx context.Context, tx any) error {
	return tx.(pgx.Tx).Commit(ctx)
}

func (c *PostgresConnector) SetupNormalizedTable(
	ctx context.Context,
	tx any,
	config *protos.SetupNormalizedTableBatchInput,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
) (bool, error) {
	createNormalizedTablesTx := tx.(pgx.Tx)

	parsedNormalizedTable, err := common.ParseTableIdentifier(tableIdentifier)
	if err != nil {
		return false, fmt.Errorf("error while parsing table schema and name: %w", err)
	}
	tableAlreadyExists, err := c.tableExists(ctx, parsedNormalizedTable)
	if err != nil {
		return false, fmt.Errorf("error occurred while checking if normalized table exists: %w", err)
	}
	if tableAlreadyExists {
		c.logger.Info("[postgres] table already exists, skipping",
			slog.String("table", tableIdentifier))
		if !config.IsResync {
			return true, nil
		}

		err := c.ExecuteCommand(ctx, fmt.Sprintf(dropTableIfExistsSQL,
			common.QuoteIdentifier(parsedNormalizedTable.Namespace),
			common.QuoteIdentifier(parsedNormalizedTable.Table)))
		if err != nil {
			return false, fmt.Errorf("error while dropping _resync table: %w", err)
		}
		c.logger.Info("[postgres] dropped resync table for resync", slog.String("resyncTable", parsedNormalizedTable.String()))
	}

	// convert the column names and types to Postgres types
	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(config, parsedNormalizedTable, tableSchema)
	_, err = c.execWithLoggingTx(ctx, normalizedTableCreateSQL, createNormalizedTablesTx)
	if err != nil {
		return false, fmt.Errorf("error while creating normalized table: %w", err)
	}

	return false, nil
}

// replayTableSchemaDeltaCore changes a destination table to match the schema at source
// This could involve adding or dropping multiple columns.
func (c *PostgresConnector) ReplayTableSchemaDeltas(
	ctx context.Context,
	_ map[string]string,
	flowJobName string,
	_ []*protos.TableMapping,
	schemaDeltas []*protos.TableSchemaDelta,
	_ []string,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	// Postgres is cool and supports transactional DDL. So we use a transaction.
	tableSchemaModifyTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for schema modification: %w",
			err)
	}
	defer shared.RollbackTx(tableSchemaModifyTx, c.logger)

	for _, schemaDelta := range schemaDeltas {
		if schemaDelta == nil || len(schemaDelta.AddedColumns) == 0 {
			continue
		}

		for _, addedColumn := range schemaDelta.AddedColumns {
			columnType := addedColumn.Type
			switch schemaDelta.System {
			case protos.TypeSystem_Q:
				columnType = qValueKindToPostgresType(columnType)
			case protos.TypeSystem_PG:
				// schema qualification handled after numeric typmod check
			default:
				return fmt.Errorf("unknown type system %d", schemaDelta.System)
			}

			if strings.EqualFold(columnType, "numeric") && addedColumn.TypeModifier != -1 {
				precision, scale := numeric.ParseNumericTypmod(addedColumn.TypeModifier)
				columnType = fmt.Sprintf("numeric(%d,%d)", precision, scale)
			} else if schemaDelta.System == protos.TypeSystem_PG && addedColumn.TypeSchemaName != "" {
				schemaQualifiedType := common.QualifiedTable{
					Namespace: addedColumn.TypeSchemaName,
					Table:     columnType,
				}
				columnType = schemaQualifiedType.String()
			}

			dstSchemaTable, err := common.ParseTableIdentifier(schemaDelta.DstTableName)
			if err != nil {
				return fmt.Errorf("error parsing schema and table for %s: %w", schemaDelta.DstTableName, err)
			}

			_, err = c.execWithLoggingTx(ctx, fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
				common.QuoteIdentifier(dstSchemaTable.Namespace),
				common.QuoteIdentifier(dstSchemaTable.Table),
				common.QuoteIdentifier(addedColumn.Name), columnType), tableSchemaModifyTx)
			if err != nil {
				return fmt.Errorf("failed to add column %s for table %s: %w", addedColumn.Name,
					schemaDelta.DstTableName, err)
			}
			c.logger.Info(fmt.Sprintf("[schema delta replay] added column %s with data type %s",
				addedColumn.Name, addedColumn.Type),
				slog.String("srcTableName", schemaDelta.SrcTableName),
				slog.String("dstTableName", schemaDelta.DstTableName),
			)
		}
	}

	if err := tableSchemaModifyTx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction for table schema modification: %w", err)
	}
	return nil
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(
	ctx context.Context,
	req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	tableIdentifierMapping := make(map[string]*protos.PostgresTableIdentifier)
	for _, tableName := range req.SourceTableIdentifiers {
		schemaTable, err := common.ParseTableIdentifier(tableName)
		if err != nil {
			return nil, fmt.Errorf("error parsing schema and table: %w", err)
		}

		// check if the table exists by getting the relation ID
		relID, err := c.getRelIDForTable(ctx, schemaTable)
		if err != nil {
			return nil, err
		}

		tableIdentifierMapping[tableName] = &protos.PostgresTableIdentifier{
			RelId: relID,
		}

		if !req.CheckConstraints {
			internal.LoggerFromCtx(ctx).Info("[no-constraints] ensured pullability table " + tableName)
			continue
		}

		replicaIdentity, replErr := c.getReplicaIdentityType(ctx, relID, schemaTable)
		if replErr != nil {
			return nil, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, replErr)
		}

		pKeyCols, err := c.getUniqueColumns(ctx, relID, replicaIdentity, schemaTable)
		if err != nil {
			return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
		}

		// we only allow no primary key if the table has REPLICA IDENTITY FULL
		// this is ok for replica identity index as we populate the primary key columns
		if len(pKeyCols) == 0 && replicaIdentity != ReplicaIdentityFull {
			return nil, exceptions.NewMissingPrimaryKeyError(schemaTable.String())
		}
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}

func (c *PostgresConnector) ExportTxSnapshot(
	ctx context.Context,
	_ string,
	env map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	skipSnapshotExport, err := internal.PeerDBSkipSnapshotExport(ctx, env)
	if err != nil {
		c.logger.Error("failed to check PEERDB_SKIP_SNAPSHOT_EXPORT, proceeding with export snapshot", slog.Any("error", err))
	} else if skipSnapshotExport {
		return &protos.ExportTxSnapshotOutput{
			SnapshotName: "",
		}, nil, err
	}

	var snapshotName string
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	needRollback := true
	defer func() {
		if needRollback {
			shared.RollbackTx(tx, c.logger)
		}
	}()

	if _, err := tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout=0"); err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting idle_in_transaction_session_timeout: %w", err)
	}

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout=0"); err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting lock_timeout: %w", err)
	}

	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotName); err != nil {
		return nil, nil, err
	}

	needRollback = false

	return &protos.ExportTxSnapshotOutput{
		SnapshotName: snapshotName,
	}, tx, err
}

func (c *PostgresConnector) FinishExport(tx any) error {
	if tx == nil {
		return nil
	}
	pgtx := tx.(pgx.Tx)
	timeout, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return pgtx.Commit(timeout)
}

// SetupReplication sets up replication for the source connector
func (c *PostgresConnector) SetupReplication(
	ctx context.Context,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	if !shared.IsValidReplicationName(req.FlowJobName) {
		return model.SetupReplicationResult{}, fmt.Errorf("invalid flow job name: `%s`, it should be ^[a-z_][a-z0-9_]*$", req.FlowJobName)
	}

	slotName := GetDefaultSlotName(req.FlowJobName)
	if req.ExistingReplicationSlotName != "" {
		slotName = req.ExistingReplicationSlotName
	}

	publicationName := GetDefaultPublicationName(req.FlowJobName)
	if req.ExistingPublicationName != "" {
		publicationName = req.ExistingPublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return model.SetupReplicationResult{}, err
	}

	skipSnapshotExport, err := internal.PeerDBSkipSnapshotExport(ctx, req.Env)
	if err != nil {
		c.logger.Error("failed to check PEERDB_SKIP_SNAPSHOT_EXPORT, proceeding with export snapshot", slog.Any("error", err))
		skipSnapshotExport = false
	}

	tableNameMapping := make(map[string]model.NameAndExclude, len(req.TableNameMapping))
	for k, v := range req.TableNameMapping {
		tableNameMapping[k] = model.NameAndExclude{
			Name:    v,
			Exclude: make(map[string]struct{}, 0),
		}
	}
	// Create the replication slot and publication
	return c.createSlotAndPublication(ctx, exists, slotName, publicationName, tableNameMapping,
		req.DoInitialSnapshot, skipSnapshotExport, req.Env)
}

func (c *PostgresConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	slotName := GetDefaultSlotName(jobName)
	if _, err := c.conn.Exec(
		ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, slotName,
	); err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}

	publicationName := GetDefaultPublicationName(jobName)

	// check if publication exists manually,
	// as drop publication if exists requires permissions
	// for a publication which we did not create via peerdb user
	var publicationExists bool
	if err := c.conn.QueryRow(
		ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", publicationName,
	).Scan(&publicationExists); err != nil {
		return fmt.Errorf("error checking if publication exists: %w", err)
	}

	if publicationExists {
		if _, err := c.conn.Exec(
			ctx, "DROP PUBLICATION IF EXISTS "+publicationName,
		); err != nil && !shared.IsSQLStateError(err, pgerrcode.ReadOnlySQLTransaction) {
			return fmt.Errorf("error dropping publication: %w", err)
		}
	}

	return nil
}

func (c *PostgresConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	syncFlowCleanupTx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to begin transaction for sync flow cleanup: %w", err)
	}
	defer shared.RollbackTx(syncFlowCleanupTx, c.logger)

	if _, err := c.execWithLoggingTx(ctx,
		fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema, getRawTableIdentifier(jobName)), syncFlowCleanupTx,
	); err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}

	mirrorJobsTableExists, err := c.tableExists(ctx, &common.QualifiedTable{
		Namespace: c.metadataSchema,
		Table:     mirrorJobsTableIdentifier,
	})
	if err != nil {
		return fmt.Errorf("unable to check if job metadata table exists: %w", err)
	}
	if mirrorJobsTableExists {
		if _, err := syncFlowCleanupTx.Exec(ctx,
			fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName,
		); err != nil {
			return fmt.Errorf("unable to delete job metadata: %w", err)
		}
	}

	if err := syncFlowCleanupTx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}

	return nil
}

func (c *PostgresConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool shared.CatalogPool,
	alertKeys *alerting.AlertKeys,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	logger := internal.LoggerFromCtx(ctx)

	slotInfos, err := getSlotInfo(ctx, c.conn, alertKeys.SlotName, c.Config.Database, false, nil)
	if err != nil {
		logger.Warn("warning: failed to get slot info", slog.Any("error", err))
		return err
	}

	if len(slotInfos) == 0 {
		logger.Warn("warning: unable to get slot info", slog.String("slotName", alertKeys.SlotName))
		return nil
	}
	slotInfo := slotInfos[0]

	logger.Info(fmt.Sprintf("Checking %s lag for %s", alertKeys.SlotName, alertKeys.PeerName),
		slog.Float64("LagInMB", float64(slotInfo.LagInMb)))
	alerter.AlertIfSlotLag(ctx, alertKeys, slotInfo)

	attributeSet := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
	))
	slotMetricGauges.SlotLagGauge.Record(ctx, float64(slotInfo.LagInMb), attributeSet)
	slotMetricGauges.RestartToConfirmedMBGauge.Record(ctx, float64(slotInfo.RestartToConfirmedMb), attributeSet)
	slotMetricGauges.ConfirmedToCurrentMBGauge.Record(ctx, float64(slotInfo.ConfirmedToCurrentMb), attributeSet)

	currentLSN, err := pglogrepl.ParseLSN(slotInfo.CurrentLSN)
	if err != nil {
		logger.Warn("error parsing current LSN", slog.Any("error", err))
	}
	slotMetricGauges.CurrentWalLSNGauge.Record(ctx, int64(currentLSN), attributeSet)

	if slotInfo.SentLSN != nil {
		sentLSN, err := pglogrepl.ParseLSN(*slotInfo.SentLSN)
		if err != nil {
			logger.Warn("error parsing sent LSN", slog.Any("error", err))
		}
		slotMetricGauges.SentLSNGauge.Record(ctx, int64(sentLSN), attributeSet)
	}

	confirmedFlushLSN, err := pglogrepl.ParseLSN(slotInfo.ConfirmedFlushLSN)
	if err != nil {
		logger.Warn("error parsing confirmed flush LSN", slog.Any("error", err))
	}
	slotMetricGauges.ConfirmedFlushLSNGauge.Record(ctx, int64(confirmedFlushLSN), attributeSet)

	restartLSN, err := pglogrepl.ParseLSN(slotInfo.RestartLSN)
	if err != nil {
		logger.Warn("error parsing restart LSN", slog.Any("error", err))
	}
	slotMetricGauges.RestartLSNGauge.Record(ctx, int64(restartLSN), attributeSet)

	if slotInfo.SafeWalSize != nil {
		slotMetricGauges.SafeWalSizeGauge.Record(ctx, *slotInfo.SafeWalSize, attributeSet)
	}

	var activeValue int64
	if slotInfo.Active {
		activeValue = 1
	}
	slotMetricGauges.SlotActiveGauge.Record(ctx, activeValue, attributeSet)

	slotMetricGauges.WalSenderStateGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
		attribute.String(otel_metrics.WaitEventTypeKey, slotInfo.WaitEventType),
		attribute.String(otel_metrics.WaitEventKey, slotInfo.WaitEvent),
		attribute.String(otel_metrics.BackendStateKey, slotInfo.BackendState),
	)))

	if slotInfo.WalStatus != "" {
		slotMetricGauges.WalStatusGauge.Record(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
			attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
			attribute.String(otel_metrics.SlotNameKey, alertKeys.SlotName),
			attribute.String(otel_metrics.WalStatusKey, slotInfo.WalStatus),
		)))
	}

	slotMetricGauges.LogicalDecodingWorkMemGauge.Record(ctx, slotInfo.LogicalDecodingWorkMemMb, attributeSet)

	// PG 16+ statistics gauges
	if slotInfo.StatsReset != nil {
		slotMetricGauges.StatsResetGauge.Record(ctx, *slotInfo.StatsReset, attributeSet)
	}
	if slotInfo.SpillTxns != nil {
		slotMetricGauges.SpillTxnsGauge.Record(ctx, *slotInfo.SpillTxns, attributeSet)
	}
	if slotInfo.SpillCount != nil {
		slotMetricGauges.SpillCountGauge.Record(ctx, *slotInfo.SpillCount, attributeSet)
	}
	if slotInfo.SpillBytes != nil {
		slotMetricGauges.SpillBytesGauge.Record(ctx, *slotInfo.SpillBytes, attributeSet)
	}

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	res, err := getOpenConnectionsForUser(ctx, c.conn, c.Config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open connections", slog.Any("error", err))
		return err
	}
	alerter.AlertIfOpenConnections(ctx, alertKeys, res)

	slotMetricGauges.OpenConnectionsGauge.Record(ctx, res.CurrentOpenConnections, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
		attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
	)))

	replicationRes, err := getOpenReplicationConnectionsForUser(ctx, c.conn, c.Config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open replication connections", slog.Any("error", err))
		return err
	}

	slotMetricGauges.OpenReplicationConnectionsGauge.Record(ctx, replicationRes.CurrentOpenConnections,
		metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
			attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
		)),
	)

	var intervalSinceLastNormalize *time.Duration
	if err := alerter.CatalogPool.QueryRow(
		ctx, "SELECT now()-last_updated_at FROM peerdb_stats.cdc_table_aggregate_counts WHERE flow_name=$1", alertKeys.FlowName,
	).Scan(&intervalSinceLastNormalize); err != nil {
		logger.Warn("failed to get interval since last normalize", slog.Any("error", err))
	}
	// what if the first normalize errors out/hangs?
	if intervalSinceLastNormalize == nil {
		logger.Warn("interval since last normalize is nil")
		return nil
	}
	if intervalSinceLastNormalize != nil {
		slotMetricGauges.IntervalSinceLastNormalizeGauge.Record(ctx, intervalSinceLastNormalize.Seconds(),
			metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowNameKey, alertKeys.FlowName),
				attribute.String(otel_metrics.PeerNameKey, alertKeys.PeerName),
			)),
		)
		alerter.AlertIfTooLongSinceLastNormalize(ctx, alertKeys, *intervalSinceLastNormalize)
	}

	return monitoring.AppendSlotSizeInfo(ctx, catalogPool, alertKeys.PeerName, slotInfo)
}

func getOpenConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	// COUNT() returns BIGINT
	var result pgtype.Int8
	if err := conn.QueryRow(ctx, getNumConnectionsForUser, user).Scan(&result); err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func getOpenReplicationConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	// COUNT() returns BIGINT
	var result pgtype.Int8
	if err := conn.QueryRow(ctx, getNumReplicationConnections, user).Scan(&result); err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	// Re-using the proto for now as the response is the same, can create later if needed
	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func (c *PostgresConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	if req == nil || len(req.AdditionalTables) == 0 {
		return nil
	}

	additionalSrcTables := make([]string, 0, len(req.AdditionalTables))
	for _, additionalTableMapping := range req.AdditionalTables {
		additionalSrcTables = append(additionalSrcTables, additionalTableMapping.SourceTableIdentifier)
	}

	// just check if we have all the tables already in the publication for custom publications
	if req.PublicationName != "" {
		rows, err := c.conn.Query(ctx,
			"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname=$1", req.PublicationName)
		if err != nil {
			return fmt.Errorf("failed to check tables in publication: %w", err)
		}

		tableNames, err := pgx.CollectRows[string](rows, pgx.RowTo)
		if err != nil {
			return fmt.Errorf("failed to check tables in publication: %w", err)
		}
		notPresentTables := shared.ArrayMinus(additionalSrcTables, tableNames)
		if len(notPresentTables) > 0 {
			return exceptions.NewTablesNotInPublicationError(notPresentTables, req.PublicationName)
		}
	} else {
		for _, additionalSrcTable := range additionalSrcTables {
			schemaTable, err := common.ParseTableIdentifier(additionalSrcTable)
			if err != nil {
				return err
			}
			_, err = c.execWithLogging(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
				common.QuoteIdentifier(GetDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already added to our publication
			if err != nil && !shared.IsSQLStateError(err, pgerrcode.DuplicateObject) {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("added table to publication",
				slog.String("publication", GetDefaultPublicationName(req.FlowJobName)),
				slog.String("table", additionalSrcTable))
		}
	}

	return nil
}

func (c *PostgresConnector) RemoveTablesFromPublication(ctx context.Context, req *protos.RemoveTablesFromPublicationInput) error {
	if req == nil || len(req.TablesToRemove) == 0 {
		return nil
	}

	tablesToRemove := make([]string, 0, len(req.TablesToRemove))
	for _, tableToRemove := range req.TablesToRemove {
		tablesToRemove = append(tablesToRemove, tableToRemove.SourceTableIdentifier)
	}

	if req.PublicationName == "" {
		for _, tableToRemove := range tablesToRemove {
			schemaTable, err := common.ParseTableIdentifier(tableToRemove)
			if err != nil {
				return err
			}
			_, err = c.execWithLogging(ctx, fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s",
				common.QuoteIdentifier(GetDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already removed from our publication
			if err != nil && !shared.IsSQLStateError(err, pgerrcode.UndefinedObject) {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("removed table from publication",
				slog.String("publication", GetDefaultPublicationName(req.FlowJobName)),
				slog.String("table", tableToRemove))
		}
	} else {
		c.logger.Info("custom publication provided, no need to remove tables",
			slog.String("publication", req.PublicationName))
	}

	return nil
}

func (c *PostgresConnector) RenameTables(
	ctx context.Context,
	req *protos.RenameTablesInput,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) (*protos.RenameTablesOutput, error) {
	renameTablesTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer shared.RollbackTx(renameTablesTx, c.logger)

	for _, renameRequest := range req.RenameTableOptions {
		srcTable, err := common.ParseTableIdentifier(renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse source %s: %w", renameRequest.CurrentName, err)
		}
		src := srcTable.String()

		resyncTableExists, err := c.checkIfTableExistsWithTx(ctx, srcTable.Namespace, srcTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if _resync table exists: %w", err)
		}

		if !resyncTableExists {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping rename", src))
			continue
		}

		dstTable, err := common.ParseTableIdentifier(renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse destination %s: %w", renameRequest.NewName, err)
		}
		dst := dstTable.String()

		// if original table does not exist, skip soft delete transfer
		originalTableExists, err := c.checkIfTableExistsWithTx(ctx, dstTable.Namespace, dstTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if source table exists: %w", err)
		}

		if originalTableExists {
			tableSchema := tableNameSchemaMapping[renameRequest.CurrentName]
			if req.SoftDeleteColName != "" {
				columnNames := make([]string, 0, len(tableSchema.Columns))
				for _, col := range tableSchema.Columns {
					columnNames = append(columnNames, common.QuoteIdentifier(col.Name))
				}

				var pkeyColCompare strings.Builder
				for _, col := range tableSchema.PrimaryKeyColumns {
					pkeyColCompare.WriteString("original_table.")
					pkeyColCompare.WriteString(common.QuoteIdentifier(col))
					pkeyColCompare.WriteString(" = resync_table.")
					pkeyColCompare.WriteString(common.QuoteIdentifier(col))
					pkeyColCompare.WriteString(" AND ")
				}
				pkeyColCompareStr := strings.TrimSuffix(pkeyColCompare.String(), " AND ")

				allCols := strings.Join(columnNames, ",")
				c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dst))
				_, err = c.execWithLoggingTx(ctx,
					fmt.Sprintf(
						"INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s original_table "+
							"WHERE NOT EXISTS (SELECT 1 FROM %s resync_table WHERE %s)",
						src, fmt.Sprintf("%s,%s", allCols, common.QuoteIdentifier(req.SoftDeleteColName)), allCols, req.SoftDeleteColName,
						dst, src, pkeyColCompareStr), renameTablesTx)
				if err != nil {
					return nil, fmt.Errorf("unable to handle soft-deletes for table %s: %w", dst, err)
				}
			}
		} else {
			c.logger.Info(fmt.Sprintf("table '%s' did not exist, skipped soft delete transfer", dst))
		}

		// renaming and dropping such that the _resync table is the new destination
		c.logger.Info(fmt.Sprintf("renaming table '%s' to '%s'...", src, dst))

		// drop the dst table if exists
		if _, err := c.execWithLoggingTx(ctx, "DROP TABLE IF EXISTS "+dst, renameTablesTx); err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		if _, err := c.execWithLoggingTx(ctx,
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", src, common.QuoteIdentifier(dstTable.Table)),
			renameTablesTx,
		); err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", src, dst, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", src, dst))
	}

	if err := renameTablesTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("unable to commit transaction for rename tables: %w", err)
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}

func (c *PostgresConnector) RemoveTableEntriesFromRawTable(
	ctx context.Context,
	req *protos.RemoveTablesFromRawTableInput,
) error {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)
	for _, tableName := range req.DestinationTableNames {
		_, err := c.execWithLogging(ctx, fmt.Sprintf("DELETE FROM %s WHERE _peerdb_destination_table_name = %s"+
			" AND _peerdb_batch_id > %d AND _peerdb_batch_id <= %d",
			common.QuoteIdentifier(rawTableIdentifier), utils.QuoteLiteral(tableName), req.NormalizeBatchId, req.SyncBatchId))
		if err != nil {
			c.logger.Error("failed to remove entries from raw table", slog.Any("error", err))
		}

		c.logger.Info(fmt.Sprintf("successfully removed entries for table '%s' from raw table", tableName))
	}

	return nil
}

func (c *PostgresConnector) GetVersion(ctx context.Context) (string, error) {
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", err
	}
	c.logger.Info("[postgres] version", slog.String("version", version))
	return version, nil
}

func (c *PostgresConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// First check for Aurora by trying to look up aurora_version()
	var isAurora bool
	err := c.conn.QueryRow(ctx, "SELECT to_regproc('pg_catalog.aurora_version') IS NOT NULL").Scan(&isAurora)
	if err != nil {
		c.logger.Error("failed to query to_regproc for determining variant", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}
	if isAurora {
		return protos.DatabaseVariant_AWS_AURORA, nil
	}

	// It's not Aurora - continue checking other variants
	settingsQuery := `
		SELECT name, setting
		FROM pg_settings
		WHERE name IN (
			'rds.extensions',
			'cloudsql.logical_decoding',
			'azure.extensions',
			'neon.endpoint_id',
			'extwlist.pscale_allowed_extensions',
			'supautils.privileged_extensions'
		) AND setting IS NOT NULL AND setting != ''`

	rows, err := c.conn.Query(ctx, settingsQuery)
	if err != nil {
		c.logger.Error("failed to query pg_settings for determining variant", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			c.logger.Warn("failed to scan from pg_settings", slog.Any("error", err))
			continue
		}

		switch name {
		case "rds.extensions":
			return protos.DatabaseVariant_AWS_RDS, nil
		case "cloudsql.logical_decoding":
			return protos.DatabaseVariant_GOOGLE_CLOUD_SQL, nil
		case "azure.extensions":
			return protos.DatabaseVariant_AZURE_DATABASE, nil
		case "neon.endpoint_id":
			return protos.DatabaseVariant_NEON, nil
		case "extwlist.pscale_allowed_extensions":
			return protos.DatabaseVariant_PLANETSCALE, nil
		case "supautils.privileged_extensions":
			return protos.DatabaseVariant_SUPABASE, nil
		}
	}

	if err := rows.Err(); err != nil {
		c.logger.Error("error iterating pg_settings rows", slog.Any("error", err))
		return protos.DatabaseVariant_VARIANT_UNKNOWN, err
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}

func (c *PostgresConnector) GetTableSizeEstimatedBytes(ctx context.Context, tableIdentifier string) (int64, error) {
	tableSizeQuery := "SELECT pg_relation_size(to_regclass($1))"
	var tableSizeBytes pgtype.Int8
	if err := c.conn.QueryRow(ctx, tableSizeQuery, tableIdentifier).Scan(&tableSizeBytes); err != nil {
		return 0, err
	}
	if !tableSizeBytes.Valid {
		return 0, errors.New("table size is not valid")
	}
	return tableSizeBytes.Int64, nil
}
