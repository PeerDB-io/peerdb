package connpostgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq/oid"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/connectors/utils/cdc_records"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/geo"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

const maxRetriesForWalSegmentRemoved = 5

type PostgresCDCSource struct {
	ctx                    context.Context
	replConn               *pgx.Conn
	SrcTableIDNameMapping  map[uint32]string
	TableNameMapping       map[string]model.NameAndExclude
	slot                   string
	SetLastOffset          func(int64) error
	publication            string
	relationMessageMapping model.RelationMessageMapping
	typeMap                *pgtype.Map
	commitLock             bool
	customTypeMapping      map[uint32]string

	// for partitioned tables, maps child relid to parent relid
	childToParentRelIDMapping map[uint32]uint32
	logger                    slog.Logger

	// for storing chema delta audit logs to catalog
	catalogPool *pgxpool.Pool
	flowJobName string

	walSegmentRemovedRegex *regexp.Regexp
}

type PostgresCDCConfig struct {
	AppContext             context.Context
	Connection             *pgx.Conn
	Slot                   string
	Publication            string
	SrcTableIDNameMapping  map[uint32]string
	TableNameMapping       map[string]model.NameAndExclude
	RelationMessageMapping model.RelationMessageMapping
	CatalogPool            *pgxpool.Pool
	FlowJobName            string
	SetLastOffset          func(int64) error
}

type startReplicationOpts struct {
	conn            *pgconn.PgConn
	startLSN        pglogrepl.LSN
	replicationOpts pglogrepl.StartReplicationOptions
}

// Create a new PostgresCDCSource
func NewPostgresCDCSource(cdcConfig *PostgresCDCConfig, customTypeMap map[uint32]string) (*PostgresCDCSource, error) {
	childToParentRelIDMap, err := getChildToParentRelIDMap(cdcConfig.AppContext, cdcConfig.Connection)
	if err != nil {
		return nil, fmt.Errorf("error getting child to parent relid map: %w", err)
	}

	pattern := "requested WAL segment .* has already been removed.*"
	regex := regexp.MustCompile(pattern)

	flowName, _ := cdcConfig.AppContext.Value(shared.FlowNameKey).(string)
	return &PostgresCDCSource{
		ctx:                       cdcConfig.AppContext,
		replConn:                  cdcConfig.Connection,
		SrcTableIDNameMapping:     cdcConfig.SrcTableIDNameMapping,
		TableNameMapping:          cdcConfig.TableNameMapping,
		slot:                      cdcConfig.Slot,
		SetLastOffset:             cdcConfig.SetLastOffset,
		publication:               cdcConfig.Publication,
		relationMessageMapping:    cdcConfig.RelationMessageMapping,
		typeMap:                   pgtype.NewMap(),
		childToParentRelIDMapping: childToParentRelIDMap,
		commitLock:                false,
		customTypeMapping:         customTypeMap,
		logger:                    *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
		catalogPool:               cdcConfig.CatalogPool,
		flowJobName:               cdcConfig.FlowJobName,
		walSegmentRemovedRegex:    regex,
	}, nil
}

func getChildToParentRelIDMap(ctx context.Context, conn *pgx.Conn) (map[uint32]uint32, error) {
	query := `
		SELECT
				parent.oid AS parentrelid,
				child.oid AS childrelid
		FROM pg_inherits
				JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
				JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
		WHERE parent.relkind='p';
	`

	rows, err := conn.Query(ctx, query, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, fmt.Errorf("error querying for child to parent relid map: %w", err)
	}

	defer rows.Close()

	childToParentRelIDMap := make(map[uint32]uint32)
	var parentRelID pgtype.Uint32
	var childRelID pgtype.Uint32
	for rows.Next() {
		err := rows.Scan(&parentRelID, &childRelID)
		if err != nil {
			return nil, fmt.Errorf("error scanning child to parent relid map: %w", err)
		}
		childToParentRelIDMap[childRelID.Uint32] = parentRelID.Uint32
	}

	return childToParentRelIDMap, nil
}

// PullRecords pulls records from the cdc stream
func (p *PostgresCDCSource) PullRecords(req *model.PullRecordsRequest) error {
	replicationOpts, err := p.replicationOptions()
	if err != nil {
		return fmt.Errorf("error getting replication options: %w", err)
	}

	pgConn := p.replConn.PgConn()
	p.logger.Info("created replication connection")

	// start replication
	var clientXLogPos, startLSN pglogrepl.LSN
	if req.LastOffset > 0 {
		p.logger.Info("starting replication from last sync state", slog.Int64("last checkpoint", req.LastOffset))
		clientXLogPos = pglogrepl.LSN(req.LastOffset)
		startLSN = clientXLogPos + 1
	}

	opts := startReplicationOpts{
		conn:            pgConn,
		startLSN:        startLSN,
		replicationOpts: *replicationOpts,
	}

	err = p.startReplication(opts)
	if err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}

	p.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", p.slot, startLSN))

	return p.consumeStream(pgConn, req, clientXLogPos, req.RecordStream)
}

func (p *PostgresCDCSource) startReplication(opts startReplicationOpts) error {
	err := pglogrepl.StartReplication(p.ctx, opts.conn, p.slot, opts.startLSN, opts.replicationOpts)
	if err != nil {
		p.logger.Error("error starting replication", slog.Any("error", err))
		return fmt.Errorf("error starting replication at startLsn - %d: %w", opts.startLSN, err)
	}

	p.logger.Info(fmt.Sprintf("started replication on slot %s at startLSN: %d", p.slot, opts.startLSN))
	return nil
}

func (p *PostgresCDCSource) replicationOptions() (*pglogrepl.StartReplicationOptions, error) {
	pluginArguments := []string{
		"proto_version '1'",
	}

	if p.publication != "" {
		pubOpt := fmt.Sprintf("publication_names '%s'", p.publication)
		pluginArguments = append(pluginArguments, pubOpt)
	} else {
		return nil, fmt.Errorf("publication name is not set")
	}

	return &pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}, nil
}

// start consuming the cdc stream
func (p *PostgresCDCSource) consumeStream(
	conn *pgconn.PgConn,
	req *model.PullRecordsRequest,
	clientXLogPos pglogrepl.LSN,
	records *model.CDCRecordStream,
) error {
	defer func() {
		err := conn.Close(p.ctx)
		if err != nil {
			p.logger.Error("error closing replication connection", slog.Any("error", err))
		}
	}()

	// clientXLogPos is the last checkpoint id, we need to ack that we have processed
	// until clientXLogPos each time we send a standby status update.
	// consumedXLogPos is the lsn that has been committed on the destination.
	consumedXLogPos := pglogrepl.LSN(0)
	if clientXLogPos > 0 {
		consumedXLogPos = clientXLogPos

		err := pglogrepl.SendStandbyStatusUpdate(p.ctx, conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: consumedXLogPos})
		if err != nil {
			return fmt.Errorf("[initial-flush] SendStandbyStatusUpdate failed: %w", err)
		}
	}

	var standByLastLogged time.Time
	cdcRecordsStorage := cdc_records.NewCDCRecordsStore(p.flowJobName)
	defer func() {
		if cdcRecordsStorage.IsEmpty() {
			records.SignalAsEmpty()
		}
		p.logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", cdcRecordsStorage.Len()))
		err := cdcRecordsStorage.Close()
		if err != nil {
			p.logger.Warn("failed to clean up records storage", slog.Any("error", err))
		}
	}()

	shutdown := utils.HeartbeatRoutine(p.ctx, func() string {
		jobName := p.flowJobName
		currRecords := cdcRecordsStorage.Len()
		return fmt.Sprintf("pulling records for job - %s, currently have %d records", jobName, currRecords)
	})
	defer shutdown()

	standbyMessageTimeout := req.IdleTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	addRecordWithKey := func(key model.TableWithPkey, rec model.Record) error {
		records.AddRecord(rec)
		err := cdcRecordsStorage.Set(key, rec)
		if err != nil {
			return err
		}

		if cdcRecordsStorage.Len() == 1 {
			records.SignalAsNotEmpty()
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			p.logger.Info(fmt.Sprintf("pushing the standby deadline to %s", nextStandbyMessageDeadline))
		}
		return nil
	}

	addRecord := func(rec model.Record) error {
		key, err := p.recToTablePKey(req, rec)
		if err != nil {
			return err
		}
		return addRecordWithKey(*key, rec)
	}

	pkmRequiresResponse := false
	waitingForCommit := false
	retryAttemptForWALSegmentRemoved := 0

	for {
		if pkmRequiresResponse {
			err := pglogrepl.SendStandbyStatusUpdate(p.ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: consumedXLogPos})
			if err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}
			pkmRequiresResponse = false

			if time.Since(standByLastLogged) > 10*time.Second {
				numRowsProcessedMessage := fmt.Sprintf("processed %d rows", cdcRecordsStorage.Len())
				p.logger.Info(fmt.Sprintf("Sent Standby status message. %s", numRowsProcessedMessage))
				standByLastLogged = time.Now()
			}
		}

		if !p.commitLock {
			if cdcRecordsStorage.Len() >= int(req.MaxBatchSize) {
				return nil
			}

			if waitingForCommit {
				p.logger.Info(fmt.Sprintf(
					"[%s] commit received, returning currently accumulated records - %d",
					p.flowJobName,
					cdcRecordsStorage.Len()),
				)
				return nil
			}
		}

		// if we are past the next standby deadline (?)
		if time.Now().After(nextStandbyMessageDeadline) {
			if !cdcRecordsStorage.IsEmpty() {
				p.logger.Info(fmt.Sprintf("[%s] standby deadline reached, have %d records, will return at next commit",
					p.flowJobName,
					cdcRecordsStorage.Len()),
				)

				if !p.commitLock {
					// immediate return if we are not waiting for a commit
					return nil
				}

				waitingForCommit = true
			} else {
				p.logger.Info(fmt.Sprintf("[%s] standby deadline reached, no records accumulated, continuing to wait",
					p.flowJobName),
				)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		var ctx context.Context
		var cancel context.CancelFunc

		if cdcRecordsStorage.IsEmpty() {
			ctx, cancel = context.WithCancel(p.ctx)
		} else {
			ctx, cancel = context.WithDeadline(p.ctx, nextStandbyMessageDeadline)
		}
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()

		utils.RecordHeartbeatWithRecover(p.ctx, "consumeStream ReceiveMessage")
		ctxErr := p.ctx.Err()
		if ctxErr != nil {
			return fmt.Errorf("consumeStream preempted: %w", ctxErr)
		}

		if err != nil && !p.commitLock {
			if pgconn.Timeout(err) {
				p.logger.Info(fmt.Sprintf("Stand-by deadline reached, returning currently accumulated records - %d",
					cdcRecordsStorage.Len()))
				return nil
			} else {
				return fmt.Errorf("ReceiveMessage failed: %w", err)
			}
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			if errMsg.Severity == "ERROR" && errMsg.Code == "XX000" {
				if p.walSegmentRemovedRegex.MatchString(errMsg.Message) {
					retryAttemptForWALSegmentRemoved++
					if retryAttemptForWALSegmentRemoved > maxRetriesForWalSegmentRemoved {
						return fmt.Errorf("max retries for WAL segment removed exceeded: %+v", errMsg)
					} else {
						p.logger.Warn(
							"WAL segment removed, restarting replication retrying in 30 seconds...",
							slog.Any("error", errMsg), slog.Int("retryAttempt", retryAttemptForWALSegmentRemoved))
						time.Sleep(30 * time.Second)
						continue
					}
				}
			}
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}

			p.logger.Debug(
				fmt.Sprintf("Primary Keepalive Message => ServerWALEnd: %s ServerTime: %s ReplyRequested: %t",
					pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested))

			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}

			// always reply to keepalive messages
			// instead of `pkm.ReplyRequested`
			pkmRequiresResponse = true

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData failed: %w", err)
			}

			p.logger.Debug(fmt.Sprintf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s\n",
				xld.WALStart, xld.ServerWALEnd, xld.ServerTime))
			rec, err := p.processMessage(records, xld, clientXLogPos)
			if err != nil {
				return fmt.Errorf("error processing message: %w", err)
			}

			if rec != nil {
				tableName := rec.GetDestinationTableName()
				switch r := rec.(type) {
				case *model.UpdateRecord:
					// tableName here is destination tableName.
					// should be ideally sourceTableName as we are in PullRecords.
					// will change in future
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						err = addRecord(rec)
						if err != nil {
							return err
						}
					} else {
						tablePkeyVal, err := p.recToTablePKey(req, rec)
						if err != nil {
							return err
						}

						latestRecord, ok, err := cdcRecordsStorage.Get(*tablePkeyVal)
						if err != nil {
							return err
						}
						if !ok {
							err = addRecordWithKey(*tablePkeyVal, rec)
						} else {
							// iterate through unchanged toast cols and set them in new record
							updatedCols := r.NewItems.UpdateIfNotExists(latestRecord.GetItems())
							for _, col := range updatedCols {
								delete(r.UnchangedToastColumns, col)
							}
							err = addRecordWithKey(*tablePkeyVal, rec)
						}
						if err != nil {
							return err
						}
					}

				case *model.InsertRecord:
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						err = addRecord(rec)
						if err != nil {
							return err
						}
					} else {
						tablePkeyVal, err := p.recToTablePKey(req, rec)
						if err != nil {
							return err
						}

						err = addRecordWithKey(*tablePkeyVal, rec)
						if err != nil {
							return err
						}
					}
				case *model.DeleteRecord:
					tablePkeyVal, err := p.recToTablePKey(req, rec)
					if err != nil {
						return err
					}

					latestRecord, ok, err := cdcRecordsStorage.Get(*tablePkeyVal)
					if err != nil {
						return err
					}
					if ok {
						deleteRecord := rec.(*model.DeleteRecord)
						deleteRecord.Items = latestRecord.GetItems()
						updateRecord, ok := latestRecord.(*model.UpdateRecord)
						if ok {
							deleteRecord.UnchangedToastColumns = updateRecord.UnchangedToastColumns
						}
					} else {
						deleteRecord := rec.(*model.DeleteRecord)
						// there is nothing to backfill the items in the delete record with,
						// so don't update the row with this record
						// add sentinel value to prevent update statements from selecting
						deleteRecord.UnchangedToastColumns = map[string]struct{}{
							"_peerdb_not_backfilled_delete": {},
						}
					}

					err = addRecord(rec)
					if err != nil {
						return err
					}
				case *model.RelationRecord:
					tableSchemaDelta := r.TableSchemaDelta
					if len(tableSchemaDelta.AddedColumns) > 0 {
						p.logger.Info(fmt.Sprintf("Detected schema change for table %s, addedColumns: %v",
							tableSchemaDelta.SrcTableName, tableSchemaDelta.AddedColumns))
						records.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
					}
				}
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}

func (p *PostgresCDCSource) processMessage(batch *model.CDCRecordStream, xld pglogrepl.XLogData,
	currentClientXlogPos pglogrepl.LSN,
) (model.Record, error) {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		p.logger.Debug(fmt.Sprintf("BeginMessage => FinalLSN: %v, XID: %v", msg.FinalLSN, msg.Xid))
		p.logger.Debug("Locking PullRecords at BeginMessage, awaiting CommitMessage")
		p.commitLock = true
	case *pglogrepl.InsertMessage:
		return p.processInsertMessage(xld.WALStart, msg)
	case *pglogrepl.UpdateMessage:
		return p.processUpdateMessage(xld.WALStart, msg)
	case *pglogrepl.DeleteMessage:
		return p.processDeleteMessage(xld.WALStart, msg)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		p.logger.Debug(fmt.Sprintf("CommitMessage => CommitLSN: %v, TransactionEndLSN: %v",
			msg.CommitLSN, msg.TransactionEndLSN))
		batch.UpdateLatestCheckpoint(int64(msg.CommitLSN))
		p.commitLock = false
	case *pglogrepl.RelationMessage:
		// treat all relation messages as corresponding to parent if partitioned.
		msg.RelationID = p.getParentRelIDIfPartitioned(msg.RelationID)

		if _, exists := p.SrcTableIDNameMapping[msg.RelationID]; !exists {
			return nil, nil
		}

		// TODO (kaushik): consider persistent state for a mirror job
		// to be stored somewhere in temporal state. We might need to persist
		// the state of the relation message somewhere
		p.logger.Debug(fmt.Sprintf("RelationMessage => RelationID: %d, Namespace: %s, RelationName: %s, Columns: %v",
			msg.RelationID, msg.Namespace, msg.RelationName, msg.Columns))
		if p.relationMessageMapping[msg.RelationID] == nil {
			p.relationMessageMapping[msg.RelationID] = convertRelationMessageToProto(msg)
		} else {
			// RelationMessages don't contain an LSN, so we use current clientXlogPos instead.
			// https://github.com/postgres/postgres/blob/8b965c549dc8753be8a38c4a1b9fabdb535a4338/src/backend/replication/logical/proto.c#L670
			return p.processRelationMessage(currentClientXlogPos, convertRelationMessageToProto(msg))
		}

	case *pglogrepl.TruncateMessage:
		p.logger.Warn("TruncateMessage not supported")
	}

	return nil, nil
}

func (p *PostgresCDCSource) processInsertMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
) (model.Record, error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("InsertMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	// create empty map of string to interface{}
	items, _, err := p.convertTupleToMap(msg.Tuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord{
		CheckpointID:         int64(lsn),
		Items:                items,
		DestinationTableName: p.TableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func (p *PostgresCDCSource) processUpdateMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
) (model.Record, error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("UpdateMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	// create empty map of string to interface{}
	oldItems, _, err := p.convertTupleToMap(msg.OldTuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, unchangedToastColumns, err := p.convertTupleToMap(msg.NewTuple,
		rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	return &model.UpdateRecord{
		CheckpointID:          int64(lsn),
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.TableNameMapping[tableName].Name,
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

// processDeleteMessage processes a delete message and returns a DeleteRecord
func (p *PostgresCDCSource) processDeleteMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
) (model.Record, error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("DeleteMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	// create empty map of string to interface{}
	items, _, err := p.convertTupleToMap(msg.OldTuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord{
		CheckpointID:         int64(lsn),
		Items:                items,
		DestinationTableName: p.TableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

/*
convertTupleToMap converts a PostgreSQL logical replication
tuple to a map representation.
It takes a tuple and a relation message as input and returns
1. a map of column names to values and
2. a string slice of unchanged TOAST column names
*/
func (p *PostgresCDCSource) convertTupleToMap(
	tuple *pglogrepl.TupleData,
	rel *protos.RelationMessage,
	exclude map[string]struct{},
) (*model.RecordItems, map[string]struct{}, error) {
	// if the tuple is nil, return an empty map
	if tuple == nil {
		return model.NewRecordItems(0), make(map[string]struct{}), nil
	}

	// create empty map of string to interface{}
	items := model.NewRecordItems(len(tuple.Columns))
	unchangedToastColumns := make(map[string]struct{})

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		if _, ok := exclude[colName]; ok {
			continue
		}
		switch col.DataType {
		case 'n': // null
			val := qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}
			items.AddColumn(colName, val)
		case 't': // text
			/* bytea also appears here as a hex */
			data, err := p.decodeColumnData(col.Data, rel.Columns[idx].DataType, pgtype.TextFormatCode)
			if err != nil {
				return nil, nil, fmt.Errorf("error decoding text column data: %w", err)
			}
			items.AddColumn(colName, data)
		case 'b': // binary
			data, err := p.decodeColumnData(col.Data, rel.Columns[idx].DataType, pgtype.BinaryFormatCode)
			if err != nil {
				return nil, nil, fmt.Errorf("error decoding binary column data: %w", err)
			}
			items.AddColumn(colName, data)
		case 'u': // unchanged toast
			unchangedToastColumns[colName] = struct{}{}
		default:
			return nil, nil, fmt.Errorf("unknown column data type: %s", string(col.DataType))
		}
	}
	return items, unchangedToastColumns, nil
}

func (p *PostgresCDCSource) decodeColumnData(data []byte, dataType uint32, formatCode int16) (qvalue.QValue, error) {
	var parsedData any
	var err error
	if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		if dt.Name == "uuid" || dt.Name == "cidr" || dt.Name == "inet" || dt.Name == "macaddr" {
			// below is required to decode above types to string
			parsedData, err = dt.Codec.DecodeDatabaseSQLValue(p.typeMap, dataType, pgtype.TextFormatCode, data)
		} else {
			parsedData, err = dt.Codec.DecodeValue(p.typeMap, dataType, formatCode, data)
		}
		if err != nil {
			return qvalue.QValue{}, err
		}
		retVal, err := parseFieldFromPostgresOID(dataType, parsedData)
		if err != nil {
			return qvalue.QValue{}, err
		}
		return retVal, nil
	} else if dataType == uint32(oid.T_timetz) { // ugly TIMETZ workaround for CDC decoding.
		retVal, err := parseFieldFromPostgresOID(dataType, string(data))
		if err != nil {
			return qvalue.QValue{}, err
		}
		return retVal, nil
	}

	typeName, ok := p.customTypeMapping[dataType]
	if ok {
		customQKind := customTypeToQKind(typeName)
		if customQKind == qvalue.QValueKindGeography || customQKind == qvalue.QValueKindGeometry {
			wkt, err := geo.GeoValidate(string(data))
			if err != nil {
				return qvalue.QValue{
					Kind:  customQKind,
					Value: nil,
				}, nil
			} else {
				return qvalue.QValue{
					Kind:  customQKind,
					Value: wkt,
				}, nil
			}
		} else {
			return qvalue.QValue{
				Kind:  customQKind,
				Value: string(data),
			}, nil
		}
	}

	return qvalue.QValue{Kind: qvalue.QValueKindString, Value: string(data)}, nil
}

func convertRelationMessageToProto(msg *pglogrepl.RelationMessage) *protos.RelationMessage {
	protoColArray := make([]*protos.RelationMessageColumn, 0)
	for _, column := range msg.Columns {
		protoColArray = append(protoColArray, &protos.RelationMessageColumn{
			Name:     column.Name,
			Flags:    uint32(column.Flags),
			DataType: column.DataType,
		})
	}
	return &protos.RelationMessage{
		RelationId:   msg.RelationID,
		RelationName: msg.RelationName,
		Columns:      protoColArray,
	}
}

func (p *PostgresCDCSource) auditSchemaDelta(flowJobName string, rec *model.RelationRecord) error {
	activityInfo := activity.GetInfo(p.ctx)
	workflowID := activityInfo.WorkflowExecution.ID
	runID := activityInfo.WorkflowExecution.RunID

	_, err := p.catalogPool.Exec(p.ctx,
		`INSERT INTO
		 peerdb_stats.schema_deltas_audit_log(flow_job_name,workflow_id,run_id,delta_info)
		 VALUES($1,$2,$3,$4)`,
		flowJobName, workflowID, runID, rec)
	if err != nil {
		return fmt.Errorf("failed to insert row into table: %w", err)
	}
	return nil
}

// processRelationMessage processes a RelationMessage and returns a TableSchemaDelta
func (p *PostgresCDCSource) processRelationMessage(
	lsn pglogrepl.LSN,
	currRel *protos.RelationMessage,
) (model.Record, error) {
	// retrieve initial RelationMessage for table changed.
	prevRel := p.relationMessageMapping[currRel.RelationId]
	// creating maps for lookup later
	prevRelMap := make(map[string]*protos.PostgresTableIdentifier)
	currRelMap := make(map[string]*protos.PostgresTableIdentifier)
	for _, column := range prevRel.Columns {
		prevRelMap[column.Name] = &protos.PostgresTableIdentifier{
			RelId: column.DataType,
		}
	}
	for _, column := range currRel.Columns {
		currRelMap[column.Name] = &protos.PostgresTableIdentifier{
			RelId: column.DataType,
		}
	}

	schemaDelta := &protos.TableSchemaDelta{
		// set it to the source table for now, so we can update the schema on the source side
		// then at the Workflow level we set it t
		SrcTableName: p.SrcTableIDNameMapping[currRel.RelationId],
		DstTableName: p.TableNameMapping[p.SrcTableIDNameMapping[currRel.RelationId]].Name,
		AddedColumns: make([]*protos.DeltaAddedColumn, 0),
	}
	for _, column := range currRel.Columns {
		// not present in previous relation message, but in current one, so added.
		if prevRelMap[column.Name] == nil {
			qKind := postgresOIDToQValueKind(column.DataType)
			if qKind == qvalue.QValueKindInvalid {
				typeName, ok := p.customTypeMapping[column.DataType]
				if ok {
					qKind = customTypeToQKind(typeName)
				}
			}
			schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.Name,
				ColumnType: string(qKind),
			})
			// present in previous and current relation messages, but data types have changed.
			// so we add it to AddedColumns and DroppedColumns, knowing that we process DroppedColumns first.
		} else if prevRelMap[column.Name].RelId != currRelMap[column.Name].RelId {
			p.logger.Warn(fmt.Sprintf("Detected dropped column %s in table %s, but not propagating", column,
				schemaDelta.SrcTableName))
		}
	}
	for _, column := range prevRel.Columns {
		// present in previous relation message, but not in current one, so dropped.
		if currRelMap[column.Name] == nil {
			p.logger.Warn(fmt.Sprintf("Detected dropped column %s in table %s, but not propagating", column,
				schemaDelta.SrcTableName))
		}
	}

	p.relationMessageMapping[currRel.RelationId] = currRel
	rec := &model.RelationRecord{
		TableSchemaDelta: schemaDelta,
		CheckpointID:     int64(lsn),
	}
	return rec, p.auditSchemaDelta(p.flowJobName, rec)
}

func (p *PostgresCDCSource) recToTablePKey(req *model.PullRecordsRequest,
	rec model.Record,
) (*model.TableWithPkey, error) {
	tableName := rec.GetDestinationTableName()
	pkeyColsMerged := make([]byte, 0)

	for _, pkeyCol := range req.TableNameSchemaMapping[tableName].PrimaryKeyColumns {
		pkeyColVal, err := rec.GetItems().GetValueByColName(pkeyCol)
		if err != nil {
			return nil, fmt.Errorf("error getting pkey column value: %w", err)
		}
		pkeyColsMerged = append(pkeyColsMerged, []byte(fmt.Sprint(pkeyColVal.Value))...)
	}

	return &model.TableWithPkey{
		TableName:  tableName,
		PkeyColVal: sha256.Sum256(pkeyColsMerged),
	}, nil
}

func (p *PostgresCDCSource) getParentRelIDIfPartitioned(relID uint32) uint32 {
	parentRelID, ok := p.childToParentRelIDMapping[relID]
	if ok {
		return parentRelID
	}

	return relID
}
