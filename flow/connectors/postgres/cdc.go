package connpostgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"slices"
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
	geo "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type PostgresCDCSource struct {
	*PostgresConnector
	srcTableIDNameMapping  map[uint32]string
	tableNameMapping       map[string]model.NameAndExclude
	tableNameSchemaMapping map[string]*protos.TableSchema
	relationMessageMapping model.RelationMessageMapping
	slot                   string
	publication            string
	typeMap                *pgtype.Map
	commitLock             *pglogrepl.BeginMessage

	// for partitioned tables, maps child relid to parent relid
	childToParentRelIDMapping map[uint32]uint32

	// for storing chema delta audit logs to catalog
	catalogPool *pgxpool.Pool
	flowJobName string
}

type PostgresCDCConfig struct {
	CatalogPool            *pgxpool.Pool
	SrcTableIDNameMapping  map[uint32]string
	TableNameMapping       map[string]model.NameAndExclude
	TableNameSchemaMapping map[string]*protos.TableSchema
	ChildToParentRelIDMap  map[uint32]uint32
	RelationMessageMapping model.RelationMessageMapping
	FlowJobName            string
	Slot                   string
	Publication            string
}

type startReplicationOpts struct {
	conn            *pgconn.PgConn
	replicationOpts pglogrepl.StartReplicationOptions
	startLSN        pglogrepl.LSN
}

// Create a new PostgresCDCSource
func (c *PostgresConnector) NewPostgresCDCSource(cdcConfig *PostgresCDCConfig) *PostgresCDCSource {
	return &PostgresCDCSource{
		PostgresConnector:         c,
		srcTableIDNameMapping:     cdcConfig.SrcTableIDNameMapping,
		tableNameMapping:          cdcConfig.TableNameMapping,
		tableNameSchemaMapping:    cdcConfig.TableNameSchemaMapping,
		relationMessageMapping:    cdcConfig.RelationMessageMapping,
		slot:                      cdcConfig.Slot,
		publication:               cdcConfig.Publication,
		childToParentRelIDMapping: cdcConfig.ChildToParentRelIDMap,
		typeMap:                   pgtype.NewMap(),
		commitLock:                nil,
		catalogPool:               cdcConfig.CatalogPool,
		flowJobName:               cdcConfig.FlowJobName,
	}
}

func GetChildToParentRelIDMap(ctx context.Context, conn *pgx.Conn) (map[uint32]uint32, error) {
	query := `
		SELECT parent.oid AS parentrelid, child.oid AS childrelid
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
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
func (p *PostgresCDCSource) PullRecords(ctx context.Context, req *model.PullRecordsRequest) error {
	logger := logger.LoggerFromCtx(ctx)
	conn := p.replConn.PgConn()
	records := req.RecordStream
	// clientXLogPos is the last checkpoint id, we need to ack that we have processed
	// until clientXLogPos each time we send a standby status update.
	// consumedXLogPos is the lsn that has been committed on the destination.
	var clientXLogPos, consumedXLogPos pglogrepl.LSN
	if req.LastOffset > 0 {
		clientXLogPos = pglogrepl.LSN(req.LastOffset)
		consumedXLogPos = clientXLogPos

		err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
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
		logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", cdcRecordsStorage.Len()))
		err := cdcRecordsStorage.Close()
		if err != nil {
			logger.Warn("failed to clean up records storage", slog.Any("error", err))
		}
	}()

	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		currRecords := cdcRecordsStorage.Len()
		msg := fmt.Sprintf("pulling records, currently have %d records", currRecords)
		logger.Info(msg)
		return msg
	})
	defer shutdown()

	standbyMessageTimeout := req.IdleTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	addRecordWithKey := func(key model.TableWithPkey, rec model.Record) error {
		err := cdcRecordsStorage.Set(logger, key, rec)
		if err != nil {
			return err
		}
		records.AddRecord(rec)

		if cdcRecordsStorage.Len() == 1 {
			records.SignalAsNotEmpty()
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			logger.Info(fmt.Sprintf("pushing the standby deadline to %s", nextStandbyMessageDeadline))
		}
		return nil
	}

	pkmRequiresResponse := false
	waitingForCommit := false

	for {
		if pkmRequiresResponse {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: consumedXLogPos})
			if err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}
			pkmRequiresResponse = false

			if time.Since(standByLastLogged) > 10*time.Second {
				numRowsProcessedMessage := fmt.Sprintf("processed %d rows", cdcRecordsStorage.Len())
				logger.Info("Sent Standby status message. " + numRowsProcessedMessage)
				standByLastLogged = time.Now()
			}
		}

		if p.commitLock == nil {
			cdclen := cdcRecordsStorage.Len()
			if cdclen >= 0 && uint32(cdclen) >= req.MaxBatchSize {
				return nil
			}

			if waitingForCommit {
				logger.Info(fmt.Sprintf(
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
				logger.Info(fmt.Sprintf("standby deadline reached, have %d records", cdcRecordsStorage.Len()))

				if p.commitLock == nil {
					logger.Info(
						fmt.Sprintf("no commit lock, returning currently accumulated records - %d",
							cdcRecordsStorage.Len()))
					return nil
				} else {
					logger.Info(fmt.Sprintf("commit lock, waiting for commit to return records - %d",
						cdcRecordsStorage.Len()))
					waitingForCommit = true
				}
			} else {
				logger.Info(fmt.Sprintf("[%s] standby deadline reached, no records accumulated, continuing to wait",
					p.flowJobName),
				)
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		var receiveCtx context.Context
		var cancel context.CancelFunc
		if cdcRecordsStorage.IsEmpty() {
			receiveCtx, cancel = context.WithCancel(ctx)
		} else {
			receiveCtx, cancel = context.WithDeadline(ctx, nextStandbyMessageDeadline)
		}
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		ctxErr := ctx.Err()
		if ctxErr != nil {
			return fmt.Errorf("consumeStream preempted: %w", ctxErr)
		}

		if err != nil && p.commitLock == nil {
			if pgconn.Timeout(err) {
				logger.Info(fmt.Sprintf("Stand-by deadline reached, returning currently accumulated records - %d",
					cdcRecordsStorage.Len()))
				return nil
			} else {
				return fmt.Errorf("ReceiveMessage failed: %w", err)
			}
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			logger.Error(fmt.Sprintf("received Postgres WAL error: %+v", errMsg))
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

			logger.Debug(
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

			logger.Debug(fmt.Sprintf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s\n",
				xld.WALStart, xld.ServerWALEnd, xld.ServerTime))
			rec, err := p.processMessage(ctx, records, xld, clientXLogPos)
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
						err := addRecordWithKey(model.TableWithPkey{}, rec)
						if err != nil {
							return err
						}
					} else {
						tablePkeyVal, err := p.recToTablePKey(req, rec)
						if err != nil {
							return err
						}

						latestRecord, ok, err := cdcRecordsStorage.Get(tablePkeyVal)
						if err != nil {
							return err
						}
						if !ok {
							err = addRecordWithKey(tablePkeyVal, rec)
						} else {
							// iterate through unchanged toast cols and set them in new record
							updatedCols := r.NewItems.UpdateIfNotExists(latestRecord.GetItems())
							for _, col := range updatedCols {
								delete(r.UnchangedToastColumns, col)
							}
							err = addRecordWithKey(tablePkeyVal, rec)
						}
						if err != nil {
							return err
						}
					}

				case *model.InsertRecord:
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						err := addRecordWithKey(model.TableWithPkey{}, rec)
						if err != nil {
							return err
						}
					} else {
						tablePkeyVal, err := p.recToTablePKey(req, rec)
						if err != nil {
							return err
						}

						err = addRecordWithKey(tablePkeyVal, rec)
						if err != nil {
							return err
						}
					}
				case *model.DeleteRecord:
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						err := addRecordWithKey(model.TableWithPkey{}, rec)
						if err != nil {
							return err
						}
					} else {
						tablePkeyVal, err := p.recToTablePKey(req, rec)
						if err != nil {
							return err
						}

						latestRecord, ok, err := cdcRecordsStorage.Get(tablePkeyVal)
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

						// A delete can only be followed by an INSERT, which does not need backfilling
						// No need to store DeleteRecords in memory or disk.
						err = addRecordWithKey(model.TableWithPkey{}, rec)
						if err != nil {
							return err
						}
					}

				case *model.RelationRecord:
					tableSchemaDelta := r.TableSchemaDelta
					if len(tableSchemaDelta.AddedColumns) > 0 {
						logger.Info(fmt.Sprintf("Detected schema change for table %s, addedColumns: %v",
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

func (p *PostgresCDCSource) baseRecord(lsn pglogrepl.LSN) model.BaseRecord {
	var nano int64
	if p.commitLock != nil {
		nano = p.commitLock.CommitTime.UnixNano()
	}
	return model.BaseRecord{
		CheckpointID:   int64(lsn),
		CommitTimeNano: nano,
	}
}

func (p *PostgresCDCSource) processMessage(
	ctx context.Context,
	batch *model.CDCRecordStream,
	xld pglogrepl.XLogData,
	currentClientXlogPos pglogrepl.LSN,
) (model.Record, error) {
	logger := logger.LoggerFromCtx(ctx)
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		logger.Debug(fmt.Sprintf("BeginMessage => FinalLSN: %v, XID: %v", msg.FinalLSN, msg.Xid))
		logger.Debug("Locking PullRecords at BeginMessage, awaiting CommitMessage")
		p.commitLock = msg
	case *pglogrepl.InsertMessage:
		return p.processInsertMessage(xld.WALStart, msg)
	case *pglogrepl.UpdateMessage:
		return p.processUpdateMessage(xld.WALStart, msg)
	case *pglogrepl.DeleteMessage:
		return p.processDeleteMessage(xld.WALStart, msg)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		logger.Debug(fmt.Sprintf("CommitMessage => CommitLSN: %v, TransactionEndLSN: %v",
			msg.CommitLSN, msg.TransactionEndLSN))
		batch.UpdateLatestCheckpoint(int64(msg.CommitLSN))
		p.commitLock = nil
	case *pglogrepl.RelationMessage:
		// treat all relation messages as corresponding to parent if partitioned.
		msg.RelationID = p.getParentRelIDIfPartitioned(msg.RelationID)

		if _, exists := p.srcTableIDNameMapping[msg.RelationID]; !exists {
			return nil, nil
		}

		logger.Debug(fmt.Sprintf("RelationMessage => RelationID: %d, Namespace: %s, RelationName: %s, Columns: %v",
			msg.RelationID, msg.Namespace, msg.RelationName, msg.Columns))

		return p.processRelationMessage(ctx, currentClientXlogPos, msg)

	case *pglogrepl.TruncateMessage:
		logger.Warn("TruncateMessage not supported")
	}

	return nil, nil
}

func (p *PostgresCDCSource) processInsertMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
) (model.Record, error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.srcTableIDNameMapping[relID]
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
	items, _, err := p.convertTupleToMap(msg.Tuple, rel, p.tableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func (p *PostgresCDCSource) processUpdateMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
) (model.Record, error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.srcTableIDNameMapping[relID]
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
	oldItems, _, err := p.convertTupleToMap(msg.OldTuple, rel, p.tableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, unchangedToastColumns, err := p.convertTupleToMap(msg.NewTuple,
		rel, p.tableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	return &model.UpdateRecord{
		BaseRecord:            p.baseRecord(lsn),
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.tableNameMapping[tableName].Name,
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

	tableName, exists := p.srcTableIDNameMapping[relID]
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
	items, _, err := p.convertTupleToMap(msg.OldTuple, rel, p.tableNameMapping[tableName].Exclude)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
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
	rel *pglogrepl.RelationMessage,
	exclude map[string]struct{},
) (model.RecordItems, map[string]struct{}, error) {
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
			val := qvalue.QValueNull(qvalue.QValueKindInvalid)
			items.AddColumn(colName, val)
		case 't': // text
			/* bytea also appears here as a hex */
			data, err := p.decodeColumnData(col.Data, rel.Columns[idx].DataType, pgtype.TextFormatCode)
			if err != nil {
				return model.RecordItems{}, nil, fmt.Errorf("error decoding text column data: %w", err)
			}
			items.AddColumn(colName, data)
		case 'b': // binary
			data, err := p.decodeColumnData(col.Data, rel.Columns[idx].DataType, pgtype.BinaryFormatCode)
			if err != nil {
				return model.RecordItems{}, nil, fmt.Errorf("error decoding binary column data: %w", err)
			}
			items.AddColumn(colName, data)
		case 'u': // unchanged toast
			unchangedToastColumns[colName] = struct{}{}
		default:
			return model.RecordItems{}, nil, fmt.Errorf("unknown column data type: %s", string(col.DataType))
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
			if dt.Name == "time" || dt.Name == "timetz" ||
				dt.Name == "timestamp" || dt.Name == "timestamptz" {
				// indicates year is more than 4 digits or something similar,
				// which you can insert into postgres,
				// but not representable by time.Time
				p.logger.Warn(fmt.Sprintf("Invalidated and hence nulled %s data: %s",
					dt.Name, string(data)))
				switch dt.Name {
				case "time":
					return qvalue.QValueNull(qvalue.QValueKindTime), nil
				case "timetz":
					return qvalue.QValueNull(qvalue.QValueKindTimeTZ), nil
				case "timestamp":
					return qvalue.QValueNull(qvalue.QValueKindTimestamp), nil
				case "timestamptz":
					return qvalue.QValueNull(qvalue.QValueKindTimestampTZ), nil
				}
			}
			return nil, err
		}
		retVal, err := p.parseFieldFromPostgresOID(dataType, parsedData)
		if err != nil {
			return nil, err
		}
		return retVal, nil
	} else if dataType == uint32(oid.T_timetz) { // ugly TIMETZ workaround for CDC decoding.
		retVal, err := p.parseFieldFromPostgresOID(dataType, string(data))
		if err != nil {
			return nil, err
		}
		return retVal, nil
	}

	typeName, ok := p.customTypesMapping[dataType]
	if ok {
		customQKind := customTypeToQKind(typeName)
		switch customQKind {
		case qvalue.QValueKindGeography, qvalue.QValueKindGeometry:
			wkt, err := geo.GeoValidate(string(data))
			if err != nil {
				return qvalue.QValueNull(customQKind), nil
			} else if customQKind == qvalue.QValueKindGeography {
				return qvalue.QValueGeography{Val: wkt}, nil
			} else {
				return qvalue.QValueGeometry{Val: wkt}, nil
			}
		case qvalue.QValueKindHStore:
			return qvalue.QValueHStore{Val: string(data)}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(data)}, nil
		default:
			return nil, fmt.Errorf("unknown custom qkind: %s", customQKind)
		}
	}

	return qvalue.QValueString{Val: string(data)}, nil
}

func (p *PostgresCDCSource) auditSchemaDelta(ctx context.Context, flowJobName string, rec *model.RelationRecord) error {
	activityInfo := activity.GetInfo(ctx)
	workflowID := activityInfo.WorkflowExecution.ID
	runID := activityInfo.WorkflowExecution.RunID

	_, err := p.catalogPool.Exec(ctx,
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
	ctx context.Context,
	lsn pglogrepl.LSN,
	currRel *pglogrepl.RelationMessage,
) (model.Record, error) {
	// not present in tables to sync, return immediately
	if _, ok := p.srcTableIDNameMapping[currRel.RelationID]; !ok {
		p.logger.Info("relid not present in srcTableIDNameMapping, skipping relation message",
			slog.Uint64("relId", uint64(currRel.RelationID)))
		return nil, nil
	}

	// retrieve current TableSchema for table changed
	// tableNameSchemaMapping uses dst table name as the key, so annoying lookup
	prevSchema := p.tableNameSchemaMapping[p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationID]].Name]
	// creating maps for lookup later
	prevRelMap := make(map[string]qvalue.QValueKind)
	currRelMap := make(map[string]qvalue.QValueKind)
	for _, column := range prevSchema.Columns {
		prevRelMap[column.Name] = qvalue.QValueKind(column.Type)
	}
	for _, column := range currRel.Columns {
		qKind := p.postgresOIDToQValueKind(column.DataType)
		if qKind == qvalue.QValueKindInvalid {
			typeName, ok := p.customTypesMapping[column.DataType]
			if ok {
				qKind = customTypeToQKind(typeName)
			}
		}
		currRelMap[column.Name] = qKind
	}

	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName: p.srcTableIDNameMapping[currRel.RelationID],
		DstTableName: p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationID]].Name,
		AddedColumns: make([]*protos.DeltaAddedColumn, 0),
	}
	for _, column := range currRel.Columns {
		// not present in previous relation message, but in current one, so added.
		if _, ok := prevRelMap[column.Name]; !ok {
			// only add to delta if not excluded
			if _, ok := p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationID]].Exclude[column.Name]; !ok {
				schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.DeltaAddedColumn{
					ColumnName: column.Name,
					ColumnType: string(currRelMap[column.Name]),
				})
			}
			// present in previous and current relation messages, but data types have changed.
			// so we add it to AddedColumns and DroppedColumns, knowing that we process DroppedColumns first.
		} else if prevRelMap[column.Name] != currRelMap[column.Name] {
			p.logger.Warn(fmt.Sprintf("Detected column %s with type changed from %s to %s in table %s, but not propagating",
				column.Name, prevRelMap[column.Name], currRelMap[column.Name], schemaDelta.SrcTableName))
		}
	}
	for _, column := range prevSchema.Columns {
		// present in previous relation message, but not in current one, so dropped.
		if _, ok := currRelMap[column.Name]; !ok {
			p.logger.Warn(fmt.Sprintf("Detected dropped column %s in table %s, but not propagating", column,
				schemaDelta.SrcTableName))
		}
	}

	p.relationMessageMapping[currRel.RelationID] = currRel
	// only log audit if there is actionable delta
	if len(schemaDelta.AddedColumns) > 0 {
		rec := &model.RelationRecord{
			BaseRecord:       p.baseRecord(lsn),
			TableSchemaDelta: schemaDelta,
		}
		return rec, p.auditSchemaDelta(ctx, p.flowJobName, rec)
	}
	return nil, nil
}

func (p *PostgresCDCSource) recToTablePKey(req *model.PullRecordsRequest,
	rec model.Record,
) (model.TableWithPkey, error) {
	tableName := rec.GetDestinationTableName()
	pkeyColsMerged := make([][]byte, 0, len(req.TableNameSchemaMapping[tableName].PrimaryKeyColumns))

	for _, pkeyCol := range req.TableNameSchemaMapping[tableName].PrimaryKeyColumns {
		pkeyColVal, err := rec.GetItems().GetValueByColName(pkeyCol)
		if err != nil {
			return model.TableWithPkey{}, fmt.Errorf("error getting pkey column value: %w", err)
		}
		pkeyColsMerged = append(pkeyColsMerged, []byte(fmt.Sprint(pkeyColVal.Value())))
	}

	return model.TableWithPkey{
		TableName:  tableName,
		PkeyColVal: sha256.Sum256(slices.Concat(pkeyColsMerged...)),
	}, nil
}

func (p *PostgresCDCSource) getParentRelIDIfPartitioned(relID uint32) uint32 {
	parentRelID, ok := p.childToParentRelIDMapping[relID]
	if ok {
		return parentRelID
	}

	return relID
}
