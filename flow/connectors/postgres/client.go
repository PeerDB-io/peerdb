package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	numeric "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

const (
	mirrorJobsTableIdentifier = "peerdb_mirror_jobs"
	createMirrorJobsTableSQL  = `CREATE TABLE IF NOT EXISTS %s.%s(mirror_job_name TEXT PRIMARY KEY,
		lsn_offset BIGINT NOT NULL,sync_batch_id BIGINT NOT NULL,normalize_batch_id BIGINT NOT NULL)`
	rawTablePrefix    = "_peerdb_raw"
	createSchemaSQL   = "CREATE SCHEMA IF NOT EXISTS %s"
	createRawTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s(_peerdb_uid uuid NOT NULL,
		_peerdb_timestamp BIGINT NOT NULL,_peerdb_destination_table_name TEXT NOT NULL,_peerdb_data JSONB NOT NULL,
		_peerdb_record_type INTEGER NOT NULL, _peerdb_match_data JSONB,_peerdb_batch_id INTEGER,
		_peerdb_unchanged_toast_columns TEXT)`
	createRawTableBatchIDIndexSQL  = "CREATE INDEX IF NOT EXISTS %s_batchid_idx ON %s.%s(_peerdb_batch_id)"
	createRawTableDstTableIndexSQL = "CREATE INDEX IF NOT EXISTS %s_dst_table_idx ON %s.%s(_peerdb_destination_table_name)"

	getLastOffsetSQL            = "SELECT lsn_offset FROM %s.%s WHERE mirror_job_name=$1"
	setLastOffsetSQL            = "UPDATE %s.%s SET lsn_offset=GREATEST(lsn_offset, $1) WHERE mirror_job_name=$2"
	getLastSyncBatchID_SQL      = "SELECT sync_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	getLastNormalizeBatchID_SQL = "SELECT normalize_batch_id FROM %s.%s WHERE mirror_job_name=$1"
	createNormalizedTableSQL    = "CREATE TABLE IF NOT EXISTS %s(%s)"
	checkTableExistsSQL         = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2)"
	upsertJobMetadataForSyncSQL = `INSERT INTO %s.%s AS j (mirror_job_name,lsn_offset,sync_batch_id,normalize_batch_id)
		VALUES ($1,$2,$3,0) ON CONFLICT(mirror_job_name) DO UPDATE SET lsn_offset=GREATEST(j.lsn_offset, EXCLUDED.lsn_offset),
		sync_batch_id=EXCLUDED.sync_batch_id`
	checkIfJobMetadataExistsSQL          = "SELECT EXISTS(SELECT * FROM %s.%s WHERE mirror_job_name=$1)"
	updateMetadataForNormalizeRecordsSQL = "UPDATE %s.%s SET normalize_batch_id=$1 WHERE mirror_job_name=$2"

	getDistinctDestinationTableNamesSQL = `SELECT DISTINCT _peerdb_destination_table_name FROM %s.%s WHERE
	_peerdb_batch_id>$1 AND _peerdb_batch_id<=$2`
	getTableNameToUnchangedToastColsSQL = `SELECT _peerdb_destination_table_name,
	ARRAY_AGG(DISTINCT _peerdb_unchanged_toast_columns) FROM %s.%s WHERE
	_peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_record_type!=2 GROUP BY _peerdb_destination_table_name`
	mergeStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id = $1 AND _peerdb_destination_table_name=$2
	)
	MERGE INTO %s dst
	USING (SELECT %s,_peerdb_record_type,_peerdb_unchanged_toast_columns FROM src_rank WHERE _peerdb_rank=1) src
	ON %s
	WHEN NOT MATCHED AND src._peerdb_record_type!=2 THEN
	INSERT (%s) VALUES (%s) %s
	WHEN MATCHED AND src._peerdb_record_type=2 THEN %s`
	fallbackUpsertStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	INSERT INTO %s (%s) SELECT %s FROM src_rank WHERE _peerdb_rank=1 AND _peerdb_record_type!=2
	ON CONFLICT (%s) DO UPDATE SET %s`
	fallbackDeleteStatementSQL = `WITH src_rank AS (
		SELECT _peerdb_data,_peerdb_record_type,_peerdb_unchanged_toast_columns,
		RANK() OVER (PARTITION BY %s ORDER BY _peerdb_timestamp DESC) AS _peerdb_rank
		FROM %s.%s WHERE _peerdb_batch_id>$1 AND _peerdb_batch_id<=$2 AND _peerdb_destination_table_name=$3
	)
	%s src_rank WHERE %s AND src_rank._peerdb_rank=1 AND src_rank._peerdb_record_type=2`

	dropTableIfExistsSQL     = "DROP TABLE IF EXISTS %s.%s"
	deleteJobMetadataSQL     = "DELETE FROM %s.%s WHERE mirror_job_name=$1"
	getNumConnectionsForUser = `SELECT COUNT(*) FROM pg_stat_activity WHERE usename=$1
	 AND application_name LIKE 'peerdb%' AND client_addr IS NOT NULL`
	getNumReplicationConnections = `select COUNT(*) from pg_stat_replication WHERE usename = $1
	 AND application_name LIKE 'peerdb%' AND client_addr IS NOT NULL`
)

type (
	ReplicaIdentityType rune
	NullableLSN         struct {
		pglogrepl.LSN
		Null bool
	}
)

const (
	ReplicaIdentityDefault ReplicaIdentityType = 'd'
	ReplicaIdentityFull    ReplicaIdentityType = 'f'
	ReplicaIdentityIndex   ReplicaIdentityType = 'i'
	ReplicaIdentityNothing ReplicaIdentityType = 'n'
)

// getRelIDForTable returns the relation ID for a table.
func (c *PostgresConnector) getRelIDForTable(ctx context.Context, schemaTable *common.QualifiedTable) (uint32, error) {
	var relID pgtype.Uint32
	err := c.conn.QueryRow(ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
		 ON n.oid = c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`,
		schemaTable.Namespace, schemaTable.Table).Scan(&relID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, shared.ErrTableDoesNotExist
		}
		return 0, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}

	return relID.Uint32, nil
}

// getReplicaIdentity returns the replica identity for a table.
func (c *PostgresConnector) getReplicaIdentityType(
	ctx context.Context,
	relID uint32,
	schemaTable *common.QualifiedTable,
) (ReplicaIdentityType, error) {
	var replicaIdentity rune
	err := c.conn.QueryRow(ctx,
		`SELECT relreplident FROM pg_class WHERE oid = $1;`,
		relID).Scan(&replicaIdentity)
	if err != nil {
		return ReplicaIdentityDefault, fmt.Errorf("error getting replica identity for table %s: %w", schemaTable, err)
	}
	if replicaIdentity == rune(ReplicaIdentityNothing) {
		return ReplicaIdentityType(replicaIdentity), exceptions.NewReplicaIdentityNothingError(schemaTable.String(), nil)
	}

	return ReplicaIdentityType(replicaIdentity), nil
}

// getUniqueColumns returns the unique columns (used to select in MERGE statement) for a given table.
// For replica identity 'd'/default, these are the primary key columns
// For replica identity 'i'/index, these are the columns in the selected index (indisreplident set)
// For replica identity 'f'/full, if there is a primary key we use that, else we return all columns
func (c *PostgresConnector) getUniqueColumns(
	ctx context.Context,
	relID uint32,
	replicaIdentity ReplicaIdentityType,
	schemaTable *common.QualifiedTable,
) ([]string, error) {
	if replicaIdentity == ReplicaIdentityIndex {
		return c.getReplicaIdentityIndexColumns(ctx, relID, schemaTable)
	}

	// Find the primary key index OID, for replica identity 'd'/default or 'f'/full
	var pkIndexOID uint32
	err := c.conn.QueryRow(ctx,
		`SELECT indexrelid FROM pg_index WHERE indrelid = $1 AND indisprimary`,
		relID).Scan(&pkIndexOID)
	if err != nil {
		// don't error out if no pkey index, this would happen in EnsurePullability or UI.
		if errors.Is(err, pgx.ErrNoRows) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("error finding primary key index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(ctx, pkIndexOID)
}

// getReplicaIdentityIndexColumns returns the columns used in the replica identity index.
func (c *PostgresConnector) getReplicaIdentityIndexColumns(
	ctx context.Context,
	relID uint32,
	schemaTable *common.QualifiedTable,
) ([]string, error) {
	var indexRelID uint32
	// Fetch the OID of the index used as the replica identity
	err := c.conn.QueryRow(ctx,
		`SELECT indexrelid FROM pg_index WHERE indrelid=$1 AND indisreplident=true`,
		relID).Scan(&indexRelID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, exceptions.NewReplicaIdentityIndexError(schemaTable.String())
		}
		return nil, fmt.Errorf("error querying replica identity index for table %s: %w", schemaTable, err)
	}

	return c.getColumnNamesForIndex(ctx, indexRelID)
}

// getColumnNamesForIndex returns the column names for a given index.
func (c *PostgresConnector) getColumnNamesForIndex(ctx context.Context, indexOID uint32) ([]string, error) {
	rows, err := c.conn.Query(ctx,
		`SELECT a.attname FROM pg_index i
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indexrelid = $1 ORDER BY array_position(i.indkey, a.attnum::int2)`,
		indexOID)
	if err != nil {
		return nil, fmt.Errorf("error getting columns for index %v: %w", indexOID, err)
	}

	cols, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, fmt.Errorf("error scanning column for index %v: %w", indexOID, err)
	}
	return cols, nil
}

func (c *PostgresConnector) getNullableColumns(ctx context.Context, relID uint32) (map[string]struct{}, error) {
	rows, err := c.conn.Query(ctx, "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = $1 AND NOT a.attnotnull", relID)
	if err != nil {
		return nil, fmt.Errorf("error getting columns for table %v: %w", relID, err)
	}

	var name string
	nullableCols := make(map[string]struct{})
	_, err = pgx.ForEachRow(rows, []any{&name}, func() error {
		nullableCols[name] = struct{}{}
		return nil
	})
	return nullableCols, err
}

func (c *PostgresConnector) tableExists(ctx context.Context, schemaTable *common.QualifiedTable) (bool, error) {
	var exists pgtype.Bool
	if err := c.conn.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT FROM pg_tables
			WHERE schemaname = $1
			AND tablename = $2
		)`,
		schemaTable.Namespace,
		schemaTable.Table,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("error checking if table exists: %w", err)
	}

	return exists.Bool, nil
}

// checkSlotAndPublication checks if the replication slot and publication exist.
func (c *PostgresConnector) checkSlotAndPublication(ctx context.Context, slot string, publication string) (SlotCheckResult, error) {
	slotExists := false
	publicationExists := false

	// Check if the replication slot exists
	var slotName pgtype.Text
	err := c.conn.QueryRow(ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&slotName)
	if err != nil {
		// check if the error is a "no rows" error
		if !errors.Is(err, pgx.ErrNoRows) {
			return SlotCheckResult{}, fmt.Errorf("error checking for replication slot - %s: %w", slot, err)
		}
	} else {
		slotExists = true
	}

	// Check if the publication exists
	var pubName pgtype.Text
	if err := c.conn.QueryRow(ctx,
		"SELECT pubname FROM pg_publication WHERE pubname = $1", publication,
	).Scan(&pubName); err != nil {
		// check if the error is a "no rows" error
		if !errors.Is(err, pgx.ErrNoRows) {
			return SlotCheckResult{}, fmt.Errorf("error checking for publication - %s: %w", publication, err)
		}
	} else {
		publicationExists = true
	}

	return SlotCheckResult{
		SlotExists:        slotExists,
		PublicationExists: publicationExists,
	}, nil
}

func getSlotInfo(
	ctx context.Context,
	conn *pgx.Conn,
	slotName string,
	database string,
	peerdbManagedOnly bool,
	customSlotNames []string,
) ([]*protos.SlotInfo, error) {
	pgversion, err := shared.GetMajorVersion(ctx, conn)
	if err != nil {
		return nil, err
	}
	walStatusSelect := "prs.wal_status"
	safeWalSizeSelect := "prs.safe_wal_size"
	if pgversion < shared.POSTGRES_13 {
		walStatusSelect = "'unknown'"
		safeWalSizeSelect = "NULL::bigint"
	}

	ldwMBSelect := "NULL::bigint"
	if pgversion >= shared.POSTGRES_13 {
		ldwMBSelect = `(
			SELECT (pg_size_bytes(setting || COALESCE(unit,'')) / 1024 / 1024)::bigint
			FROM pg_settings WHERE name='logical_decoding_work_mem'
		)`
	}

	statsSelect := `
		NULL::bigint,
		NULL::bigint,
		NULL::bigint,
		NULL::bigint
	`
	statsJoin := ""
	if pgversion >= shared.POSTGRES_16 {
		statsSelect = `
			EXTRACT(EPOCH FROM psrs.stats_reset)::bigint,
			psrs.spill_txns,
			psrs.spill_count,
			psrs.spill_bytes
		`
		statsJoin = `
			LEFT JOIN pg_stat_replication_slots AS psrs
				ON psrs.slot_name = prs.slot_name
		`
	}
	var whereClause string
	if slotName != "" {
		whereClause = "WHERE prs.slot_name=" + utils.QuoteLiteral(slotName)
	} else {
		whereClause = "WHERE prs.database=" + utils.QuoteLiteral(database)
		if peerdbManagedOnly {
			var slotFilter strings.Builder
			slotFilter.WriteString("prs.slot_name LIKE ")
			slotFilter.WriteString(utils.QuoteLiteral(DefaultSlotPrefix + "%"))
			for _, name := range customSlotNames {
				slotFilter.WriteString(" OR prs.slot_name=")
				slotFilter.WriteString(utils.QuoteLiteral(name))
			}
			whereClause += " AND (" + slotFilter.String() + ")"
		}
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(`
		WITH current_wal AS (
			SELECT CASE
				WHEN pg_is_in_recovery()
				THEN pg_last_wal_receive_lsn()
				ELSE pg_current_wal_lsn()
			END AS current_lsn
		)
		SELECT
			prs.slot_name,
			pcc.redo_lsn::text,
			prs.restart_lsn::text,
			cw.current_lsn::text,
			%s, -- prs.wal_status
			%s, -- prs.safe_wal_size
			prs.confirmed_flush_lsn::text,
			psr.sent_lsn::text,
			prs.active,
			round((cw.current_lsn - prs.restart_lsn) / 1024 / 1024),
			round((prs.confirmed_flush_lsn - prs.restart_lsn) / 1024 / 1024),
			round((cw.current_lsn - prs.confirmed_flush_lsn) / 1024 / 1024),
			psa.wait_event_type,
			psa.wait_event,
			psa.state,
			%s, -- logical_decoding_work_mem megabytes
			%s  -- stats
		FROM current_wal cw,
			pg_control_checkpoint() as pcc,
			(pg_replication_slots as prs
				LEFT JOIN pg_stat_activity as psa
					on psa.pid = prs.active_pid
				LEFT JOIN pg_stat_replication as psr
					on psr.pid = prs.active_pid
				%s)
		%s`,
		walStatusSelect,
		safeWalSizeSelect,
		ldwMBSelect,
		statsSelect,
		statsJoin,
		whereClause,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to read information for slots: %w", err)
	}
	defer rows.Close()
	var slotInfoRows []*protos.SlotInfo
	for rows.Next() {
		var slotName pgtype.Text
		var redoLSN pgtype.Text
		var restartLSN pgtype.Text
		var currentLSN pgtype.Text
		var walStatus pgtype.Text
		var safeWalSize *int64
		var confirmedFlushLSN pgtype.Text
		var sentLSN *string
		var active pgtype.Bool
		var lagInMB pgtype.Float4
		var restartToConfirmedMB pgtype.Float4
		var confirmedToCurrentMB pgtype.Float4
		var waitEventType pgtype.Text
		var waitEvent pgtype.Text
		var backendState pgtype.Text
		var ldwMemMB pgtype.Int8
		var statsReset *int64
		var spillTxns *int64
		var spillCount *int64
		var spillBytes *int64

		err := rows.Scan(
			&slotName,
			&redoLSN,
			&restartLSN,
			&currentLSN,
			&walStatus,
			&safeWalSize,
			&confirmedFlushLSN,
			&sentLSN,
			&active,
			&lagInMB,
			&restartToConfirmedMB,
			&confirmedToCurrentMB,
			&waitEventType,
			&waitEvent,
			&backendState,
			&ldwMemMB,
			&statsReset,
			&spillTxns,
			&spillCount,
			&spillBytes,
		)
		if err != nil {
			return nil, err
		}

		slotInfoRows = append(slotInfoRows, &protos.SlotInfo{
			SlotName:                 slotName.String,
			RedoLSN:                  redoLSN.String,
			RestartLSN:               restartLSN.String,
			CurrentLSN:               currentLSN.String,
			Active:                   active.Bool,
			LagInMb:                  lagInMB.Float32,
			ConfirmedFlushLSN:        confirmedFlushLSN.String,
			SentLSN:                  sentLSN,
			RestartToConfirmedMb:     restartToConfirmedMB.Float32,
			ConfirmedToCurrentMb:     confirmedToCurrentMB.Float32,
			WalStatus:                walStatus.String,
			SafeWalSize:              safeWalSize,
			WaitEventType:            waitEventType.String,
			WaitEvent:                waitEvent.String,
			BackendState:             backendState.String,
			LogicalDecodingWorkMemMb: ldwMemMB.Int64,
			StatsReset:               statsReset,
			SpillTxns:                spillTxns,
			SpillCount:               spillCount,
			SpillBytes:               spillBytes,
		})
	}
	return slotInfoRows, nil
}

// GetSlotInfo gets the information about the replication slot size and LSNs.
// If slotName is non-empty, only that slot is returned.
// If slotName is empty and peerdbManagedOnly is false, all slots in the database are returned.
// If slotName is empty and peerdbManagedOnly is true, only slots with the peerflow_slot_ prefix
// (plus any explicitly listed customSlotNames) are returned.
func (c *PostgresConnector) GetSlotInfo(
	ctx context.Context,
	slotName string,
	peerdbManagedOnly bool,
	customSlotNames []string,
) ([]*protos.SlotInfo, error) {
	return getSlotInfo(ctx, c.conn, slotName, c.Config.Database, peerdbManagedOnly, customSlotNames)
}

func (c *PostgresConnector) CreatePublication(
	ctx context.Context,
	srcTableNames []string,
	publication string,
) error {
	tableNameString := strings.Join(srcTableNames, ", ")
	// check and enable publish_via_partition_root
	pgversion, err := c.MajorVersion(ctx)
	if err != nil {
		return fmt.Errorf("[publication-creation] error checking Postgres version: %w", err)
	}
	var pubViaRootString string
	if pgversion >= shared.POSTGRES_13 {
		pubViaRootString = " WITH(publish_via_partition_root=true)"
	}
	// Create the publication to help filter changes only for the given tables
	stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s%s", publication, tableNameString, pubViaRootString)
	if _, err = c.execWithLogging(ctx, stmt); err != nil {
		c.logger.Warn("error creating publication", slog.String("publication", publication), slog.Any("error", err))
		return fmt.Errorf("error creating publication %s: %w", publication, err)
	}
	return nil
}

// createSlotAndPublication creates the replication slot and publication.
func (c *PostgresConnector) createSlotAndPublication(
	ctx context.Context,
	s SlotCheckResult,
	slot string,
	publication string,
	tableNameMapping map[string]model.NameAndExclude,
	doInitialCopy bool,
	skipSnapshotExport bool,
	env map[string]string,
) (model.SetupReplicationResult, error) {
	// iterate through source tables and create publication,
	// expecting tablenames to be schema qualified
	if !s.PublicationExists {
		srcTableNames := make([]string, 0, len(tableNameMapping))
		for srcTableName := range tableNameMapping {
			parsedSrcTableName, err := common.ParseTableIdentifier(srcTableName)
			if err != nil {
				return model.SetupReplicationResult{}, fmt.Errorf("[publication-creation] source table identifier %s is invalid", srcTableName)
			}
			srcTableNames = append(srcTableNames, parsedSrcTableName.String())
		}
		if err := c.CreatePublication(ctx, srcTableNames, publication); err != nil {
			return model.SetupReplicationResult{}, err
		}
	}

	// create slot only after we succeeded in creating publication.
	if !s.SlotExists {
		conn, err := c.CreateReplConn(ctx, env)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error acquiring connection: %w", err)
		}

		// THIS IS NOT IN A TX!
		if _, err := conn.Exec(ctx, "SET idle_in_transaction_session_timeout=0"); err != nil {
			conn.Close(ctx)
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error setting idle_in_transaction_session_timeout: %w", err)
		}

		if _, err := conn.Exec(ctx, "SET lock_timeout=0"); err != nil {
			conn.Close(ctx)
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error setting lock_timeout: %w", err)
		}

		pgversion, err := c.MajorVersion(ctx)
		if err != nil {
			conn.Close(ctx)
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error getting PG version: %w", err)
		}

		var optionsString string
		if failoverEnabled, err := internal.PeerDBPostgresEnableFailoverSlots(ctx, env); err != nil {
			conn.Close(ctx)
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error checking dynamic config for failover slots: %w", err)
		} else if failoverEnabled {
			// can't create failover slots on a standby
			isInRecovery, err := c.IsInRecovery(ctx)
			if err != nil {
				conn.Close(ctx)
				return model.SetupReplicationResult{}, fmt.Errorf("[slot] error checking if in recovery: %w", err)
			}

			if pgversion >= shared.POSTGRES_17 && !isInRecovery {
				optionsString = " (FAILOVER 'true')"
			}
		}

		createSlotCommand := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput%s", common.QuoteIdentifier(slot), optionsString)

		c.logger.Info("Creating replication slot", slog.String("slot", slot))
		// CreateReplicationSlot does not support failover options and uses Postgres syntax that makes it tricky to drop in
		// TODO: upstream pglogrepl to support this
		res, err := pglogrepl.ParseCreateReplicationSlot(conn.PgConn().Exec(ctx, createSlotCommand))
		if err != nil {
			conn.Close(ctx)
			return model.SetupReplicationResult{}, fmt.Errorf("[slot] error creating replication slot: %w", err)
		}
		c.logger.Info("Created replication slot", slog.String("slot", slot))

		if skipSnapshotExport {
			conn.Close(ctx)
			return model.SetupReplicationResult{
				Conn:             nil,
				SlotName:         res.SlotName,
				SnapshotName:     "",
				SupportsTIDScans: pgversion >= shared.POSTGRES_13,
			}, nil
		}
		return model.SetupReplicationResult{
			Conn:             conn,
			SlotName:         res.SlotName,
			SnapshotName:     res.SnapshotName,
			SupportsTIDScans: pgversion >= shared.POSTGRES_13,
		}, nil
	} else {
		c.logger.Info("Replication slot already exists", slog.String("slot", slot))
		var err error
		if doInitialCopy {
			err = shared.ErrSlotAlreadyExists
		}
		return model.SetupReplicationResult{SlotName: slot}, err
	}
}

func (c *PostgresConnector) createMetadataSchema(ctx context.Context) error {
	if _, err := c.execWithLogging(ctx,
		fmt.Sprintf(createSchemaSQL, c.metadataSchema),
	); err != nil && !shared.IsSQLStateError(err, pgerrcode.UniqueViolation) {
		return fmt.Errorf("error while creating internal schema: %w", err)
	}
	return nil
}

func getRawTableIdentifier(jobName string) string {
	return rawTablePrefix + "_" + strings.ToLower(shared.ReplaceIllegalCharactersWithUnderscores(jobName))
}

func generateCreateTableSQLForNormalizedTable(
	config *protos.SetupNormalizedTableBatchInput,
	dstSchemaTable *common.QualifiedTable,
	tableSchema *protos.TableSchema,
) string {
	createTableSQLArray := make([]string, 0, len(tableSchema.Columns)+2)
	for _, column := range tableSchema.Columns {
		pgColumnType := column.Type

		// handle schema-qualified custom types first (for TypeSystem_PG)
		if tableSchema.System == protos.TypeSystem_PG && column.TypeSchemaName != "" {
			schemaQualifiedPgType := common.QualifiedTable{
				Namespace: column.TypeSchemaName,
				Table:     pgColumnType,
			}
			pgColumnType = schemaQualifiedPgType.String()
		} else if tableSchema.System == protos.TypeSystem_Q {
			pgColumnType = qValueKindToPostgresType(pgColumnType)
		}

		if column.Type == "numeric" && column.TypeModifier != -1 {
			precision, scale := numeric.ParseNumericTypmod(column.TypeModifier)
			pgColumnType = fmt.Sprintf("numeric(%d,%d)", precision, scale)
		}

		var notNull string
		if tableSchema.NullableEnabled && !column.Nullable {
			notNull = " NOT NULL"
		}

		createTableSQLArray = append(createTableSQLArray,
			fmt.Sprintf("%s %s%s", common.QuoteIdentifier(column.Name), pgColumnType, notNull))
	}

	if config.SoftDeleteColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			common.QuoteIdentifier(config.SoftDeleteColName)+` BOOL DEFAULT FALSE`)
	}

	if config.SyncedAtColName != "" {
		createTableSQLArray = append(createTableSQLArray,
			common.QuoteIdentifier(config.SyncedAtColName)+` TIMESTAMP DEFAULT CURRENT_TIMESTAMP`)
	}

	// add composite primary key to the table
	if len(tableSchema.PrimaryKeyColumns) > 0 && !tableSchema.IsReplicaIdentityFull {
		primaryKeyColsQuoted := make([]string, 0, len(tableSchema.PrimaryKeyColumns))
		for _, primaryKeyCol := range tableSchema.PrimaryKeyColumns {
			primaryKeyColsQuoted = append(primaryKeyColsQuoted, common.QuoteIdentifier(primaryKeyCol))
		}
		createTableSQLArray = append(createTableSQLArray, fmt.Sprintf("PRIMARY KEY(%s)",
			strings.Join(primaryKeyColsQuoted, ",")))
	}

	return fmt.Sprintf(createNormalizedTableSQL, dstSchemaTable.String(), strings.Join(createTableSQLArray, ","))
}

func (c *PostgresConnector) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	if err := c.conn.QueryRow(ctx, fmt.Sprintf(
		getLastSyncBatchID_SQL,
		c.metadataSchema,
		mirrorJobsTableIdentifier,
	), jobName).Scan(&result); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Info("No row found, returning 0")
			return 0, nil
		}
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *PostgresConnector) GetLastNormalizeBatchID(ctx context.Context, jobName string) (int64, error) {
	var result pgtype.Int8
	if err := c.conn.QueryRow(ctx, fmt.Sprintf(
		getLastNormalizeBatchID_SQL,
		c.metadataSchema,
		mirrorJobsTableIdentifier,
	), jobName).Scan(&result); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.logger.Info("No row found, returning 0")
			return 0, nil
		}
		return 0, fmt.Errorf("error while reading result row: %w", err)
	}
	return result.Int64, nil
}

func (c *PostgresConnector) jobMetadataExists(ctx context.Context, jobName string) (bool, error) {
	var result pgtype.Bool
	if err := c.conn.QueryRow(ctx,
		fmt.Sprintf(checkIfJobMetadataExistsSQL, c.metadataSchema, mirrorJobsTableIdentifier), jobName,
	).Scan(&result); err != nil {
		return false, fmt.Errorf("error reading result row: %w", err)
	}
	return result.Bool, nil
}

func (c *PostgresConnector) MajorVersion(ctx context.Context) (shared.PGVersion, error) {
	if c.pgVersion == 0 {
		pgVersion, err := shared.GetMajorVersion(ctx, c.conn)
		if err != nil {
			return 0, err
		}
		c.pgVersion = pgVersion
	}
	return c.pgVersion, nil
}

func (c *PostgresConnector) updateSyncMetadata(ctx context.Context, flowJobName string, lastCP model.CdcCheckpoint, syncBatchID int64,
	syncRecordsTx pgx.Tx,
) error {
	if _, err := syncRecordsTx.Exec(ctx,
		fmt.Sprintf(upsertJobMetadataForSyncSQL, c.metadataSchema, mirrorJobsTableIdentifier),
		flowJobName, lastCP.ID, syncBatchID,
	); err != nil {
		return fmt.Errorf("failed to upsert flow job status: %w", err)
	}

	return nil
}

func (c *PostgresConnector) getDistinctTableNamesInBatch(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
	tableToSchema map[string]*protos.TableSchema,
) ([]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.conn.Query(ctx, fmt.Sprintf(getDistinctDestinationTableNamesSQL, c.metadataSchema,
		rawTableIdentifier), normalizeBatchID, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}

	destinationTableNames, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	return slices.DeleteFunc(destinationTableNames, func(name string) bool {
		if _, ok := tableToSchema[name]; !ok {
			c.logger.Warn("table not found in table to schema mapping", "table", name)
			return true
		}
		return false
	}), nil
}

func (c *PostgresConnector) getTableNametoUnchangedCols(
	ctx context.Context,
	flowJobName string,
	syncBatchID int64,
	normalizeBatchID int64,
) (map[string][]string, error) {
	rawTableIdentifier := getRawTableIdentifier(flowJobName)

	rows, err := c.conn.Query(ctx, fmt.Sprintf(getTableNameToUnchangedToastColsSQL, c.metadataSchema,
		rawTableIdentifier), normalizeBatchID, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("error while retrieving table names for normalization: %w", err)
	}
	defer rows.Close()

	// Create a map to store the results
	resultMap := make(map[string][]string)
	var destinationTableName pgtype.Text
	var unchangedToastColumns []string
	// Process the rows and populate the map
	for rows.Next() {
		err := rows.Scan(&destinationTableName, &unchangedToastColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		resultMap[destinationTableName.String] = unchangedToastColumns
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}
	return resultMap, nil
}

func (c *PostgresConnector) getCurrentLSN(ctx context.Context) (NullableLSN, error) {
	row := c.conn.QueryRow(ctx,
		"SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END")
	var result pgtype.Text
	if err := row.Scan(&result); err != nil {
		return NullableLSN{}, fmt.Errorf("error while running query for current LSN: %w", err)
	}
	if !result.Valid || result.String == "" {
		return NullableLSN{Null: true}, nil
	}
	lsn, err := pglogrepl.ParseLSN(result.String)
	if err != nil {
		return NullableLSN{}, fmt.Errorf("error while parsing LSN %s: %w", result.String, err)
	}
	return NullableLSN{LSN: lsn}, nil
}

const DefaultSlotPrefix = "peerflow_slot_"

func GetDefaultSlotName(jobName string) string {
	return DefaultSlotPrefix + jobName
}

func GetDefaultPublicationName(jobName string) string {
	return "peerflow_pub_" + jobName
}

func (c *PostgresConnector) checkIfTableExistsWithTx(
	ctx context.Context,
	schemaName string,
	tableName string,
	tx pgx.Tx,
) (bool, error) {
	row := tx.QueryRow(ctx, checkTableExistsSQL, schemaName, tableName)
	var result pgtype.Bool
	err := row.Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while running query: %w", err)
	}

	return result.Bool, nil
}

func (c *PostgresConnector) ExecuteCommand(ctx context.Context, command string) error {
	_, err := c.conn.Exec(ctx, command)
	return err
}

func (c *PostgresConnector) execWithLogging(ctx context.Context, query string) (pgconn.CommandTag, error) {
	c.logger.Info("[postgres] executing DDL statement", slog.String("query", query))
	return c.conn.Exec(ctx, query)
}

func (c *PostgresConnector) execWithLoggingTx(ctx context.Context, query string, tx pgx.Tx) (pgconn.CommandTag, error) {
	c.logger.Info("[postgres] executing DDL statement", slog.String("query", query))
	return tx.Exec(ctx, query)
}

func (c *PostgresConnector) IsInRecovery(ctx context.Context) (bool, error) {
	var inRecovery bool
	if err := c.conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return false, fmt.Errorf("error checking if in recovery: %w", err)
	}
	return inRecovery, nil
}
