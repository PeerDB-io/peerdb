package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/otel_metrics/peerdb_guages"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type PostgresConnector struct {
	logger                 log.Logger
	config                 *protos.PostgresConfig
	ssh                    *SSHTunnel
	conn                   *pgx.Conn
	replConfig             *pgx.ConnConfig
	replConn               *pgx.Conn
	replState              *ReplState
	customTypesMapping     map[uint32]string
	hushWarnOID            map[uint32]struct{}
	relationMessageMapping model.RelationMessageMapping
	connStr                string
	metadataSchema         string
	replLock               sync.Mutex
}

type ReplState struct {
	Slot        string
	Publication string
	Offset      int64
	LastOffset  atomic.Int64
}

func NewPostgresConnector(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	connectionString := shared.GetPGConnectionString(pgConfig)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	connConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	replConfig := connConfig.Copy()
	runtimeParams := connConfig.Config.RuntimeParams
	runtimeParams["idle_in_transaction_session_timeout"] = "0"
	runtimeParams["statement_timeout"] = "0"

	tunnel, err := NewSSHTunnel(ctx, pgConfig.SshConfig)
	if err != nil {
		logger.Error("failed to create ssh tunnel", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create ssh tunnel: %w", err)
	}

	conn, err := tunnel.NewPostgresConnFromConfig(ctx, connConfig)
	if err != nil {
		logger.Error("failed to create connection", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	// ensure that replication is set to database
	replConfig.Config.RuntimeParams["replication"] = "database"
	replConfig.Config.RuntimeParams["bytea_output"] = "hex"
	replConfig.Config.RuntimeParams["intervalstyle"] = "postgres"

	customTypeMap, err := shared.GetCustomDataTypes(ctx, conn)
	if err != nil {
		logger.Error("failed to get custom type map", slog.Any("error", err))
		return nil, fmt.Errorf("failed to get custom type map: %w", err)
	}

	metadataSchema := "_peerdb_internal"
	if pgConfig.MetadataSchema != nil {
		metadataSchema = *pgConfig.MetadataSchema
	}

	return &PostgresConnector{
		connStr:                connectionString,
		config:                 pgConfig,
		ssh:                    tunnel,
		conn:                   conn,
		replConfig:             replConfig,
		replState:              nil,
		replLock:               sync.Mutex{},
		customTypesMapping:     customTypeMap,
		metadataSchema:         metadataSchema,
		hushWarnOID:            make(map[uint32]struct{}),
		logger:                 logger,
		relationMessageMapping: make(model.RelationMessageMapping),
	}, nil
}

func (c *PostgresConnector) CreateReplConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := c.ssh.NewPostgresConnFromConfig(ctx, c.replConfig)
	if err != nil {
		logger.LoggerFromCtx(ctx).Error("failed to create replication connection", "error", err)
		return nil, fmt.Errorf("failed to create replication connection: %w", err)
	}
	return conn, nil
}

func (c *PostgresConnector) SetupReplConn(ctx context.Context) error {
	conn, err := c.CreateReplConn(ctx)
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
) error {
	if c.replState != nil && (c.replState.Offset != lastOffset ||
		c.replState.Slot != slotName ||
		c.replState.Publication != publicationName) {
		msg := fmt.Sprintf("replState changed, reset connector. slot name: old=%s new=%s, publication: old=%s new=%s, offset: old=%d new=%d",
			c.replState.Slot, slotName, c.replState.Publication, publicationName, c.replState.Offset, lastOffset,
		)
		c.logger.Info(msg)
		return temporal.NewNonRetryableApplicationError(msg, "desync", nil)
	}

	if c.replState == nil {
		replicationOpts, err := c.replicationOptions(ctx, publicationName)
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
		if err := pglogrepl.StartReplication(ctx, c.replConn.PgConn(), slotName, startLSN, replicationOpts); err != nil {
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

func (c *PostgresConnector) replicationOptions(ctx context.Context, publicationName string) (pglogrepl.StartReplicationOptions, error) {
	pluginArguments := append(make([]string, 0, 3), "proto_version '1'")

	if publicationName != "" {
		pubOpt := "publication_names " + QuoteLiteral(publicationName)
		pluginArguments = append(pluginArguments, pubOpt)
	} else {
		return pglogrepl.StartReplicationOptions{}, errors.New("publication name is not set")
	}

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return pglogrepl.StartReplicationOptions{}, err
	} else if pgversion >= shared.POSTGRES_14 {
		pluginArguments = append(pluginArguments, "messages 'true'")
	}

	return pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	var connerr, replerr error
	if c != nil {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		connerr = c.conn.Close(timeout)

		if c.replConn != nil {
			timeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			replerr = c.replConn.Close(timeout)
		}

		c.ssh.Close()
	}
	return errors.Join(connerr, replerr)
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
func (c *PostgresConnector) NeedsSetupMetadataTables(ctx context.Context) bool {
	result, err := c.tableExists(ctx, &utils.SchemaTable{
		Schema: c.metadataSchema,
		Table:  mirrorJobsTableIdentifier,
	})
	if err != nil {
		return true
	}
	return !result
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables(ctx context.Context) error {
	err := c.createMetadataSchema(ctx)
	if err != nil {
		return err
	}

	_, err = c.conn.Exec(ctx, fmt.Sprintf(createMirrorJobsTableSQL,
		c.metadataSchema, mirrorJobsTableIdentifier))
	if err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation) {
		return fmt.Errorf("error creating table %s: %w", mirrorJobsTableIdentifier, err)
	}

	return nil
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	err := c.conn.QueryRow(ctx, fmt.Sprintf(getLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName).Scan(&result)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.logger.Info("No row found, returning nil")
			return 0, nil
		}
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}

	if result.Int64 == 0 {
		c.logger.Warn("Assuming zero offset means no sync has happened")
	}
	return result.Int64, nil
}

// SetLastOffset updates the last synced offset for a job.
func (c *PostgresConnector) SetLastOffset(ctx context.Context, jobName string, lastOffset int64) error {
	_, err := c.conn.
		Exec(ctx, fmt.Sprintf(setLastOffsetSQL, c.metadataSchema, mirrorJobsTableIdentifier), lastOffset, jobName)
	if err != nil {
		return fmt.Errorf("error setting last offset for job %s: %w", jobName, err)
	}

	return nil
}

func (c *PostgresConnector) PullRecords(
	ctx context.Context,
	catalogPool *pgxpool.Pool,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	return pullCore(ctx, c, catalogPool, req, qProcessor{})
}

func (c *PostgresConnector) PullPg(
	ctx context.Context,
	catalogPool *pgxpool.Pool,
	req *model.PullRecordsRequest[model.PgItems],
) error {
	return pullCore(ctx, c, catalogPool, req, pgProcessor{})
}

// PullRecords pulls records from the source.
func pullCore[Items model.Items](
	ctx context.Context,
	c *PostgresConnector,
	catalogPool *pgxpool.Pool,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
) error {
	defer func() {
		req.RecordStream.Close()
		if c.replState != nil {
			c.replState.Offset = req.RecordStream.GetLastCheckpoint()
		}
	}()

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + req.FlowJobName
	if req.OverrideReplicationSlotName != "" {
		slotName = req.OverrideReplicationSlotName
	}

	publicationName := c.getDefaultPublicationName(req.FlowJobName)
	if req.OverridePublicationName != "" {
		publicationName = req.OverridePublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
	}

	if !exists.PublicationExists {
		c.logger.Warn(fmt.Sprintf("publication %s does not exist", publicationName))
		publicationName = ""
	}

	if !exists.SlotExists {
		c.logger.Warn(fmt.Sprintf("slot %s does not exist", slotName))
		return fmt.Errorf("replication slot %s does not exist", slotName)
	}

	c.logger.Info("PullRecords: performed checks for slot and publication")

	childToParentRelIDMap, err := GetChildToParentRelIDMap(ctx, c.conn)
	if err != nil {
		return fmt.Errorf("error getting child to parent relid map: %w", err)
	}

	if err := c.MaybeStartReplication(ctx, slotName, publicationName, req.LastOffset); err != nil {
		c.logger.Error("error starting replication", slog.Any("error", err))
		return err
	}

	cdc := c.NewPostgresCDCSource(&PostgresCDCConfig{
		SrcTableIDNameMapping:  req.SrcTableIDNameMapping,
		Slot:                   slotName,
		Publication:            publicationName,
		TableNameMapping:       req.TableNameMapping,
		TableNameSchemaMapping: req.TableNameSchemaMapping,
		ChildToParentRelIDMap:  childToParentRelIDMap,
		CatalogPool:            catalogPool,
		FlowJobName:            req.FlowJobName,
		RelationMessageMapping: c.relationMessageMapping,
	})

	if err := PullCdcRecords(ctx, cdc, req, processor, &c.replLock); err != nil {
		c.logger.Error("error pulling records", slog.Any("error", err))
		return err
	}

	latestLSN, err := c.getCurrentLSN(ctx)
	if err != nil {
		c.logger.Error("error getting current LSN", slog.Any("error", err))
		return fmt.Errorf("failed to get current LSN: %w", err)
	}

	err = monitoring.UpdateLatestLSNAtSourceForCDCFlow(ctx, catalogPool, req.FlowJobName, int64(latestLSN))
	if err != nil {
		c.logger.Error("error updating latest LSN at source for CDC flow", slog.Any("error", err))
		return fmt.Errorf("failed to update latest LSN at source for CDC flow: %w", err)
	}

	return nil
}

func (c *PostgresConnector) UpdateReplStateLastOffset(lastOffset int64) {
	if c.replState != nil {
		c.replState.LastOffset.Store(lastOffset)
	}
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
					uuid.New().String(),
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
					uuid.New().String(),
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
					uuid.New().String(),
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
	err = c.updateSyncMetadata(ctx, req.FlowJobName, lastCP, req.SyncBatchID, syncRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = syncRecordsTx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	err = c.ReplayTableSchemaDeltas(ctx, req.FlowJobName, req.Records.SchemaDeltas)
	if err != nil {
		return nil, fmt.Errorf("failed to sync schema changes: %w", err)
	}

	return &model.SyncResponse{
		LastSyncedCheckpointID: lastCP,
		NumRecordsSynced:       numRecords,
		CurrentSyncBatchID:     req.SyncBatchID,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}

func (c *PostgresConnector) NormalizeRecords(
	ctx context.Context,
	req *model.NormalizeRecordsRequest,
) (*model.NormalizeResponse, error) {
	rawTableIdentifier := getRawTableIdentifier(req.FlowJobName)

	jobMetadataExists, err := c.jobMetadataExists(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	// no SyncFlow has run, chill until more records are loaded.
	if !jobMetadataExists {
		c.logger.Info("no metadata found for mirror")
		return &model.NormalizeResponse{
			Done: false,
		}, nil
	}

	normBatchID, err := c.GetLastNormalizeBatchID(ctx, req.FlowJobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch for the current mirror: %v", err)
	}

	// normalize has caught up with sync, chill until more records are loaded.
	if normBatchID >= req.SyncBatchID {
		c.logger.Info(fmt.Sprintf("no records to normalize: syncBatchID %d, normalizeBatchID %d",
			req.SyncBatchID, normBatchID))
		return &model.NormalizeResponse{
			Done:         false,
			StartBatchID: normBatchID,
			EndBatchID:   req.SyncBatchID,
		}, nil
	}

	destinationTableNames, err := c.getDistinctTableNamesInBatch(
		ctx, req.FlowJobName, req.SyncBatchID, normBatchID)
	if err != nil {
		return nil, err
	}
	unchangedToastColumnsMap, err := c.getTableNametoUnchangedCols(ctx, req.FlowJobName,
		req.SyncBatchID, normBatchID)
	if err != nil {
		return nil, err
	}

	normalizeRecordsTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for normalizing records: %w", err)
	}
	defer shared.RollbackTx(normalizeRecordsTx, c.logger)

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return nil, err
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

	for _, destinationTableName := range destinationTableNames {
		normalizeStatements := normalizeStmtGen.generateNormalizeStatements(destinationTableName)
		for _, normalizeStatement := range normalizeStatements {
			ct, err := normalizeRecordsTx.Exec(ctx, normalizeStatement, normBatchID, req.SyncBatchID, destinationTableName)
			if err != nil {
				c.logger.Error("error executing normalize statement",
					slog.String("statement", normalizeStatement),
					slog.Int64("normBatchID", normBatchID),
					slog.Int64("syncBatchID", req.SyncBatchID),
					slog.String("destinationTableName", destinationTableName),
					slog.Any("error", err),
				)
				return nil, fmt.Errorf("error executing normalize statement for table %s: %w", destinationTableName, err)
			}
			totalRowsAffected += int(ct.RowsAffected())
		}
	}
	c.logger.Info(fmt.Sprintf("normalized %d records", totalRowsAffected))

	// updating metadata with new normalizeBatchID
	err = c.updateNormalizeMetadata(ctx, req.FlowJobName, req.SyncBatchID, normalizeRecordsTx)
	if err != nil {
		return nil, err
	}
	// transaction commits
	err = normalizeRecordsTx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &model.NormalizeResponse{
		Done:         true,
		StartBatchID: normBatchID + 1,
		EndBatchID:   req.SyncBatchID,
	}, nil
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

	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableSQL, c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableBatchIDIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating batch ID index on raw table: %w", err)
	}
	_, err = createRawTableTx.Exec(ctx, fmt.Sprintf(createRawTableDstTableIndexSQL, rawTableIdentifier,
		c.metadataSchema, rawTableIdentifier))
	if err != nil {
		return nil, fmt.Errorf("error creating destination table index on raw table: %w", err)
	}

	err = createRawTableTx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for creating raw table: %w", err)
	}

	return nil, nil
}

func (c *PostgresConnector) GetTableSchema(
	ctx context.Context,
	req *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	res := make(map[string]*protos.TableSchema)
	for _, tableName := range req.TableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(ctx, req.Env, tableName, req.System)
		if err != nil {
			c.logger.Info("error fetching schema for table "+tableName, slog.Any("error", err))
			return nil, err
		}
		res[tableName] = tableSchema
		c.logger.Info("fetched schema for table " + tableName)
	}

	return &protos.GetTableSchemaBatchOutput{
		TableNameSchemaMapping: res,
	}, nil
}

func (c *PostgresConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tableName string,
	system protos.TypeSystem,
) (*protos.TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
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

	nullableEnabled, err := peerdbenv.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	var nullableCols map[string]struct{}
	if nullableEnabled {
		nullableCols, err = c.getNullableColumns(ctx, relID)
		if err != nil {
			return nil, err
		}
	}

	// Get the column names and types
	rows, err := c.conn.Query(ctx,
		fmt.Sprintf(`SELECT * FROM %s LIMIT 0`, schemaTable.String()),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	columnNames := make([]string, 0, len(fields))
	columns := make([]*protos.FieldDescription, 0, len(fields))
	for _, fieldDescription := range fields {
		var colType string
		switch system {
		case protos.TypeSystem_PG:
			colType = c.postgresOIDToName(fieldDescription.DataTypeOID)
			if colType == "" {
				typeName, ok := c.customTypesMapping[fieldDescription.DataTypeOID]
				if !ok {
					return nil, fmt.Errorf("error getting type name for %d", fieldDescription.DataTypeOID)
				}
				colType = typeName
			}
		case protos.TypeSystem_Q:
			qColType := c.postgresOIDToQValueKind(fieldDescription.DataTypeOID)
			if qColType == qvalue.QValueKindInvalid {
				typeName, ok := c.customTypesMapping[fieldDescription.DataTypeOID]
				if ok {
					qColType = customTypeToQKind(typeName)
				} else {
					qColType = qvalue.QValueKindString
				}
			}
			colType = string(qColType)
		}

		columnNames = append(columnNames, fieldDescription.Name)
		_, nullable := nullableCols[fieldDescription.Name]
		columns = append(columns, &protos.FieldDescription{
			Name:         fieldDescription.Name,
			Type:         colType,
			TypeModifier: fieldDescription.TypeModifier,
			Nullable:     nullable,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table schema: %w", err)
	}
	// if we have no pkey, we will use all columns as the pkey for the MERGE statement
	if replicaIdentityType == ReplicaIdentityFull && len(pKeyCols) == 0 {
		pKeyCols = columnNames
	}

	return &protos.TableSchema{
		TableIdentifier:       tableName,
		PrimaryKeyColumns:     pKeyCols,
		IsReplicaIdentityFull: replicaIdentityType == ReplicaIdentityFull,
		Columns:               columns,
		NullableEnabled:       nullableEnabled,
		System:                system,
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
	env map[string]string,
	tableIdentifier string,
	tableSchema *protos.TableSchema,
	softDeleteColName string,
	syncedAtColName string,
	isResync bool,
) (bool, error) {
	createNormalizedTablesTx := tx.(pgx.Tx)

	parsedNormalizedTable, err := utils.ParseSchemaTable(tableIdentifier)
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
		if isResync {
			err := c.ExecuteCommand(ctx, fmt.Sprintf(dropTableIfExistsSQL,
				QuoteIdentifier(parsedNormalizedTable.Schema),
				QuoteIdentifier(parsedNormalizedTable.Table)))
			if err != nil {
				return false, fmt.Errorf("error while dropping _resync table: %w", err)
			}
		}
		return true, nil
	}

	// convert the column names and types to Postgres types
	normalizedTableCreateSQL := generateCreateTableSQLForNormalizedTable(
		parsedNormalizedTable.String(), tableSchema, softDeleteColName, syncedAtColName)
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
	flowJobName string,
	schemaDeltas []*protos.TableSchemaDelta,
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
			if schemaDelta.System == protos.TypeSystem_Q {
				columnType = qValueKindToPostgresType(columnType)
			}

			dstSchemaTable, err := utils.ParseSchemaTable(schemaDelta.DstTableName)
			if err != nil {
				return fmt.Errorf("error parsing schema and table for %s: %w", schemaDelta.DstTableName, err)
			}

			_, err = c.execWithLoggingTx(ctx, fmt.Sprintf(
				"ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
				QuoteIdentifier(dstSchemaTable.Schema),
				QuoteIdentifier(dstSchemaTable.Table),
				QuoteIdentifier(addedColumn.Name), columnType), tableSchemaModifyTx)
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
		schemaTable, err := utils.ParseSchemaTable(tableName)
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
			logger.LoggerFromCtx(ctx).Info("[no-constraints] ensured pullability table " + tableName)
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
			return nil, fmt.Errorf("table %s has no primary keys and does not have REPLICA IDENTITY FULL", schemaTable)
		}
	}

	return &protos.EnsurePullabilityBatchOutput{TableIdentifierMapping: tableIdentifierMapping}, nil
}

func (c *PostgresConnector) ExportTxSnapshot(ctx context.Context) (*protos.ExportTxSnapshotOutput, any, error) {
	var snapshotName string
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	txNeedsRollback := true
	defer func() {
		if txNeedsRollback {
			rollbackCtx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancelFunc()
			err := tx.Rollback(rollbackCtx)
			if err != pgx.ErrTxClosed {
				c.logger.Error("error while rolling back transaction for snapshot export")
			}
		}
	}()

	_, err = tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout=0")
	if err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting idle_in_transaction_session_timeout: %w", err)
	}

	_, err = tx.Exec(ctx, "SET LOCAL lock_timeout=0")
	if err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error setting lock_timeout: %w", err)
	}

	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("[export-snapshot] error getting PG version: %w", err)
	}

	err = tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotName)
	if err != nil {
		return nil, nil, err
	}
	txNeedsRollback = false

	return &protos.ExportTxSnapshotOutput{
		SnapshotName:     snapshotName,
		SupportsTidScans: pgversion >= shared.POSTGRES_13,
	}, tx, err
}

func (c *PostgresConnector) FinishExport(tx any) error {
	pgtx := tx.(pgx.Tx)
	timeout, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return pgtx.Commit(timeout)
}

// SetupReplication sets up replication for the source connector.
func (c *PostgresConnector) SetupReplication(ctx context.Context, signal SlotSignal, req *protos.SetupReplicationInput) error {
	if !shared.IsValidReplicationName(req.FlowJobName) {
		return fmt.Errorf("invalid flow job name: `%s`, it should be ^[a-z_][a-z0-9_]*$", req.FlowJobName)
	}

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + req.FlowJobName
	if req.ExistingReplicationSlotName != "" {
		slotName = req.ExistingReplicationSlotName
	}

	publicationName := c.getDefaultPublicationName(req.FlowJobName)
	if req.ExistingPublicationName != "" {
		publicationName = req.ExistingPublicationName
	}

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(ctx, slotName, publicationName)
	if err != nil {
		return err
	}

	tableNameMapping := make(map[string]model.NameAndExclude)
	for k, v := range req.TableNameMapping {
		tableNameMapping[k] = model.NameAndExclude{
			Name:    v,
			Exclude: make(map[string]struct{}, 0),
		}
	}
	// Create the replication slot and publication
	err = c.createSlotAndPublication(ctx, signal, exists,
		slotName, publicationName, tableNameMapping, req.DoInitialSnapshot)
	if err != nil {
		return fmt.Errorf("error creating replication slot and publication: %w", err)
	}

	return nil
}

func (c *PostgresConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := "peerflow_slot_" + jobName
	_, err := c.conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
	 WHERE slot_name=$1`, slotName)
	if err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}

	publicationName := c.getDefaultPublicationName(jobName)

	// check if publication exists manually,
	// as drop publication if exists requires permissions
	// for a publication which we did not create via peerdb user
	var publicationExists bool
	err = c.conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", publicationName).Scan(&publicationExists)
	if err != nil {
		return fmt.Errorf("error checking if publication exists: %w", err)
	}

	if publicationExists {
		_, err = c.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+publicationName)
		if err != nil {
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

	_, err = c.execWithLoggingTx(ctx, fmt.Sprintf(dropTableIfExistsSQL, c.metadataSchema,
		getRawTableIdentifier(jobName)), syncFlowCleanupTx)
	if err != nil {
		return fmt.Errorf("unable to drop raw table: %w", err)
	}

	mirrorJobsTableExists, err := c.jobMetadataExists(ctx, jobName)
	if err != nil {
		return fmt.Errorf("unable to check if job metadata exists: %w", err)
	}
	if mirrorJobsTableExists {
		_, err = syncFlowCleanupTx.Exec(ctx,
			fmt.Sprintf(deleteJobMetadataSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName)
		if err != nil {
			return fmt.Errorf("unable to delete job metadata: %w", err)
		}
	}

	err = syncFlowCleanupTx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("unable to commit transaction for sync flow cleanup: %w", err)
	}
	return nil
}

func (c *PostgresConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool *pgxpool.Pool,
	slotName string,
	peerName string,
	slotMetricGuages peerdb_guages.SlotMetricGuages,
) error {
	logger := logger.LoggerFromCtx(ctx)

	slotInfo, err := getSlotInfo(ctx, c.conn, slotName, c.config.Database)
	if err != nil {
		logger.Warn("warning: failed to get slot info", "error", err)
		return err
	}

	if len(slotInfo) == 0 {
		logger.Warn("warning: unable to get slot info", "slotName", slotName)
		return nil
	}

	logger.Info(fmt.Sprintf("Checking %s lag for %s", slotName, peerName), slog.Float64("LagInMB", float64(slotInfo[0].LagInMb)))
	alerter.AlertIfSlotLag(ctx, peerName, slotInfo[0])
	slotMetricGuages.SlotLagGuage.Set(float64(slotInfo[0].LagInMb), attribute.NewSet(
		attribute.String(peerdb_guages.PeerNameKey, peerName),
		attribute.String(peerdb_guages.SlotNameKey, slotName),
		attribute.String(peerdb_guages.DeploymentUidKey, peerdbenv.PeerDBDeploymentUID())))

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	res, err := getOpenConnectionsForUser(ctx, c.conn, c.config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open connections", "error", err)
		return err
	}
	alerter.AlertIfOpenConnections(ctx, peerName, res)
	slotMetricGuages.OpenConnectionsGuage.Set(res.CurrentOpenConnections, attribute.NewSet(
		attribute.String(peerdb_guages.PeerNameKey, peerName),
		attribute.String(peerdb_guages.DeploymentUidKey, peerdbenv.PeerDBDeploymentUID())))

	replicationRes, err := getOpenReplicationConnectionsForUser(ctx, c.conn, c.config.User)
	if err != nil {
		logger.Warn("warning: failed to get current open replication connections", "error", err)
		return err
	}

	slotMetricGuages.OpenReplicationConnectionsGuage.Set(replicationRes.CurrentOpenConnections, attribute.NewSet(
		attribute.String(peerdb_guages.PeerNameKey, peerName),
		attribute.String(peerdb_guages.DeploymentUidKey, peerdbenv.PeerDBDeploymentUID())))

	return monitoring.AppendSlotSizeInfo(ctx, catalogPool, peerName, slotInfo[0])
}

func getOpenConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	row := conn.QueryRow(ctx, getNumConnectionsForUser, user)

	// COUNT() returns BIGINT
	var result pgtype.Int8
	err := row.Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func getOpenReplicationConnectionsForUser(ctx context.Context, conn *pgx.Conn, user string) (*protos.GetOpenConnectionsForUserResult, error) {
	row := conn.QueryRow(ctx, getNumReplicationConnections, user)

	// COUNT() returns BIGINT
	var result pgtype.Int8
	err := row.Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("error while reading result row: %w", err)
	}

	// Re-using the proto for now as the response is the same, can create later if needed
	return &protos.GetOpenConnectionsForUserResult{
		UserName:               user,
		CurrentOpenConnections: result.Int64,
	}, nil
}

func (c *PostgresConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	// don't modify custom publications
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
			return fmt.Errorf("some additional tables not present in custom publication: %s",
				strings.Join(notPresentTables, ", "))
		}
	} else {
		for _, additionalSrcTable := range additionalSrcTables {
			schemaTable, err := utils.ParseSchemaTable(additionalSrcTable)
			if err != nil {
				return err
			}
			_, err = c.execWithLogging(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
				utils.QuoteIdentifier(c.getDefaultPublicationName(req.FlowJobName)),
				schemaTable.String()))
			// don't error out if table is already added to our publication
			if err != nil && !shared.IsSQLStateError(err, pgerrcode.DuplicateObject) {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
			c.logger.Info("added table to publication",
				slog.String("publication", c.getDefaultPublicationName(req.FlowJobName)),
				slog.String("table", additionalSrcTable))
		}
	}

	return nil
}

func (c *PostgresConnector) RenameTables(ctx context.Context, req *protos.RenameTablesInput) (*protos.RenameTablesOutput, error) {
	renameTablesTx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction for rename tables: %w", err)
	}
	defer shared.RollbackTx(renameTablesTx, c.logger)

	for _, renameRequest := range req.RenameTableOptions {
		srcTable, err := utils.ParseSchemaTable(renameRequest.CurrentName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse source %s: %w", renameRequest.CurrentName, err)
		}
		src := srcTable.String()

		resyncTableExists, err := c.checkIfTableExistsWithTx(ctx, srcTable.Schema, srcTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if _resync table exists: %w", err)
		}

		if !resyncTableExists {
			c.logger.Info(fmt.Sprintf("table '%s' does not exist, skipping rename", src))
			continue
		}

		dstTable, err := utils.ParseSchemaTable(renameRequest.NewName)
		if err != nil {
			return nil, fmt.Errorf("unable to parse destination %s: %w", renameRequest.NewName, err)
		}
		dst := dstTable.String()

		// if original table does not exist, skip soft delete transfer
		originalTableExists, err := c.checkIfTableExistsWithTx(ctx, dstTable.Schema, dstTable.Table, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to check if source table exists: %w", err)
		}

		if originalTableExists {
			if req.SoftDeleteColName != "" {
				columnNames := make([]string, 0, len(renameRequest.TableSchema.Columns))
				for _, col := range renameRequest.TableSchema.Columns {
					columnNames = append(columnNames, QuoteIdentifier(col.Name))
				}

				pkeyColumnNames := make([]string, 0, len(renameRequest.TableSchema.PrimaryKeyColumns))
				for _, col := range renameRequest.TableSchema.PrimaryKeyColumns {
					pkeyColumnNames = append(pkeyColumnNames, QuoteIdentifier(col))
				}

				allCols := strings.Join(columnNames, ",")
				pkeyCols := strings.Join(pkeyColumnNames, ",")

				c.logger.Info(fmt.Sprintf("handling soft-deletes for table '%s'...", dst))

				_, err = c.execWithLoggingTx(ctx,
					fmt.Sprintf("INSERT INTO %s(%s) SELECT %s,true AS %s FROM %s WHERE (%s) NOT IN (SELECT %s FROM %s)",
						src, fmt.Sprintf("%s,%s", allCols, QuoteIdentifier(req.SoftDeleteColName)), allCols, req.SoftDeleteColName,
						dst, pkeyCols, pkeyCols, src), renameTablesTx)
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
		_, err = c.execWithLoggingTx(ctx, "DROP TABLE IF EXISTS "+dst, renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to drop table %s: %w", dst, err)
		}

		// rename the src table to dst
		_, err = c.execWithLoggingTx(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", src, dstTable.Table), renameTablesTx)
		if err != nil {
			return nil, fmt.Errorf("unable to rename table %s to %s: %w", src, dst, err)
		}

		c.logger.Info(fmt.Sprintf("successfully renamed table '%s' to '%s'", src, dst))
	}

	err = renameTablesTx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to commit transaction for rename tables: %w", err)
	}

	return &protos.RenameTablesOutput{
		FlowJobName: req.FlowJobName,
	}, nil
}
