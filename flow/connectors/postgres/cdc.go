package connpostgres

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq/oid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

type postgresCDCSource struct {
	ctx                    context.Context
	replPool               *pgxpool.Pool
	srcTableIDNameMapping  map[uint32]string
	tableNameMapping       map[string]string
	slot                   string
	publication            string
	relationMessageMapping model.RelationMessageMapping
	typeMap                *pgtype.Map
	startLSN               pglogrepl.LSN
	schemas                []string
	allowTableAdditions    bool
}

type postgresCDCConfig struct {
	appContext             context.Context
	connection             *pgxpool.Pool
	slot                   string
	publication            string
	srcTableIDNameMapping  map[uint32]string
	tableNameMapping       map[string]string
	relationMessageMapping model.RelationMessageMapping
	schemas                []string
	allowTableAdditions    bool
}

// Create a new PostgresCDCSource
func NewPostgresCDCSource(cdcConfig *postgresCDCConfig) (*postgresCDCSource, error) {
	return &postgresCDCSource{
		ctx:                    cdcConfig.appContext,
		replPool:               cdcConfig.connection,
		srcTableIDNameMapping:  cdcConfig.srcTableIDNameMapping,
		tableNameMapping:       cdcConfig.tableNameMapping,
		slot:                   cdcConfig.slot,
		publication:            cdcConfig.publication,
		relationMessageMapping: cdcConfig.relationMessageMapping,
		typeMap:                pgtype.NewMap(),
		schemas:                cdcConfig.schemas,
		allowTableAdditions:    cdcConfig.allowTableAdditions,
	}, nil
}

// PullRecords pulls records from the cdc stream
func (p *postgresCDCSource) PullRecords(req *model.PullRecordsRequest) (
	*model.RecordsWithDeltaInfo, error) {
	// setup options
	pluginArguments := []string{
		"proto_version '1'",
	}

	if p.publication != "" {
		pubOpt := fmt.Sprintf("publication_names '%s'", p.publication)
		pluginArguments = append(pluginArguments, pubOpt)
	}

	replicationOpts := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}
	replicationSlot := p.slot

	// create replication connection
	replicationConn, err := p.replPool.Acquire(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("error acquiring connection for replication: %w", err)
	}

	defer replicationConn.Release()

	pgConn := replicationConn.Conn().PgConn()
	log.WithFields(log.Fields{
		"flowName": req.FlowJobName,
	}).Infof("created replication connection")

	sysident, err := pglogrepl.IdentifySystem(p.ctx, pgConn)
	if err != nil {
		return nil, fmt.Errorf("IdentifySystem failed: %w", err)
	}
	log.Debugf("SystemID: %s, Timeline: %d, XLogPos: %d, DBName: %s",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// start replication
	p.startLSN = 0
	if req.LastSyncState != nil && req.LastSyncState.Checkpoint > 0 {
		log.Infof("starting replication from last sync state - %d", req.LastSyncState.Checkpoint)
		p.startLSN = pglogrepl.LSN(req.LastSyncState.Checkpoint + 1)
	}

	err = pglogrepl.StartReplication(p.ctx, pgConn, replicationSlot, p.startLSN, replicationOpts)
	if err != nil {
		return nil, fmt.Errorf("error starting replication at startLsn - %d: %w", p.startLSN, err)
	}
	log.WithFields(log.Fields{
		"flowName": req.FlowJobName,
	}).Infof("started replication on slot %s at startLSN: %d", p.slot, p.startLSN)

	return p.consumeStream(pgConn, req, p.startLSN)
}

// start consuming the cdc stream
func (p *postgresCDCSource) consumeStream(
	conn *pgconn.PgConn,
	req *model.PullRecordsRequest,
	clientXLogPos pglogrepl.LSN,
) (*model.RecordsWithDeltaInfo, error) {
	// TODO (kaushik): take into consideration the MaxBatchSize
	// parameters in the original request.
	records := &model.RecordBatch{
		Records:           make([]model.Record, 0),
		TablePKeyLastSeen: make(map[model.TableWithPkey]int),
	}
	result := &model.RecordsWithDeltaInfo{
		RecordBatch:            records,
		TableSchemaDelta:       nil,
		RelationMessageMapping: p.relationMessageMapping,
	}

	standbyMessageTimeout := req.IdleTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	earlyReturn := false

	defer func() {
		err := conn.Close(p.ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"flowName": req.FlowJobName,
			}).Errorf("unexpected error closing replication connection: %v", err)
		}
	}()

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			// update the WALWritePosition to be clientXLogPos - 1
			// as the clientXLogPos is the last checkpoint id + 1
			// and we want to send the last checkpoint id as the last
			// checkpoint id that we have processed.
			lastProcessedXLogPos := clientXLogPos
			if clientXLogPos > 0 {
				lastProcessedXLogPos = clientXLogPos - 1
			}
			err := pglogrepl.SendStandbyStatusUpdate(p.ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: lastProcessedXLogPos})
			if err != nil {
				return nil, fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}

			numRowsProcessedMessage := fmt.Sprintf("processed %d rows", len(records.Records))
			utils.RecordHeartbeatWithRecover(p.ctx, numRowsProcessedMessage)
			log.Infof("Sent Standby status message. %s", numRowsProcessedMessage)
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(p.ctx, nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				log.Infof("Idle timeout reached, returning currently accumulated records")
				return result, nil
			}
			return nil, fmt.Errorf("ReceiveMessage failed: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return nil, fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Warnf("unexpected message type: %T", rawMsg)
			continue
		}

		firstProcessed := false

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}

			log.Debugf("Primary Keepalive Message => ServerWALEnd: %s ServerTime: %s ReplyRequested: %t",
				pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("ParseXLogData failed: %w", err)
			}

			log.Debugf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s\n",
				xld.WALStart, xld.ServerWALEnd, xld.ServerTime)
			rec, err := p.processMessage(records, xld)

			if err != nil {
				return nil, fmt.Errorf("error processing message: %w", err)
			}

			if !firstProcessed {
				firstProcessed = true
				records.FirstCheckPointID = int64(xld.WALStart)
			}
			if rec != nil {
				tableName := rec.GetTableName()
				switch r := rec.(type) {
				case *model.UpdateRecord:
					// tableName here is destination tableName.
					// should be ideally sourceTableName as we are in PullRecords.
					// will change in future
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						records.Records = append(records.Records, rec)
					} else {
						pkeyCol := req.TableNameSchemaMapping[tableName].PrimaryKeyColumn
						pkeyColVal, err := rec.GetItems().GetValueByColName(pkeyCol)
						if err != nil {
							return nil, fmt.Errorf("error getting pkey column value: %w", err)
						}

						tablePkeyVal := model.TableWithPkey{
							TableName:  tableName,
							PkeyColVal: *pkeyColVal,
						}
						_, ok := records.TablePKeyLastSeen[tablePkeyVal]
						if !ok {
							records.Records = append(records.Records, rec)
							records.TablePKeyLastSeen[tablePkeyVal] = len(records.Records) - 1
						} else {
							oldRec := records.Records[records.TablePKeyLastSeen[tablePkeyVal]]
							// iterate through unchanged toast cols and set them in new record
							updatedCols := r.NewItems.UpdateIfNotExists(oldRec.GetItems())
							for _, col := range updatedCols {
								delete(r.UnchangedToastColumns, col)
							}
							records.Records = append(records.Records, rec)
							records.TablePKeyLastSeen[tablePkeyVal] = len(records.Records) - 1
						}
					}
				case *model.InsertRecord:
					isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
					if isFullReplica {
						records.Records = append(records.Records, rec)
					} else {
						pkeyCol := req.TableNameSchemaMapping[tableName].PrimaryKeyColumn
						pkeyColVal, err := rec.GetItems().GetValueByColName(pkeyCol)
						if err != nil {
							return nil, fmt.Errorf("error getting pkey column value: %w", err)
						}
						tablePkeyVal := model.TableWithPkey{
							TableName:  tableName,
							PkeyColVal: *pkeyColVal,
						}
						records.Records = append(records.Records, rec)
						// all columns will be set in insert record, so add it to the map
						records.TablePKeyLastSeen[tablePkeyVal] = len(records.Records) - 1
					}
				case *model.DeleteRecord:
					records.Records = append(records.Records, rec)
				case *model.RelationRecord:
					tableSchemaDelta := rec.(*model.RelationRecord).TableSchemaDelta
					if len(tableSchemaDelta.AddedColumns) > 0 || len(tableSchemaDelta.DroppedColumns) > 0 {
						result.TableSchemaDelta = tableSchemaDelta
						log.Infof("Detected schema change for table %s, returning currently accumulated records",
							result.TableSchemaDelta.SrcTableName)
						earlyReturn = true
					}
				case *model.AddedTableRecord:
					log.Infof("Detected additional table %s, returning currently accumulated records",
						rec.GetTableName())
					result.AdditionalTableInfo = &protos.AdditionalTableInfo{
						TableName: rec.(*model.AddedTableRecord).TableName,
						SrcSchema: rec.(*model.AddedTableRecord).SrcSchema,
					}
					earlyReturn = true
				}
			}

			currentPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			records.LastCheckPointID = int64(currentPos)

			if records.Records != nil &&
				((len(records.Records) == int(req.MaxBatchSize)) || earlyReturn) {
				return result, nil
			}
		}
	}
}

func (p *postgresCDCSource) processMessage(batch *model.RecordBatch, xld pglogrepl.XLogData) (model.Record, error) {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		log.Debugf("Ignoring BeginMessage")
	case *pglogrepl.InsertMessage:
		return p.processInsertMessage(xld.WALStart, msg)
	case *pglogrepl.UpdateMessage:
		return p.processUpdateMessage(xld.WALStart, msg)
	case *pglogrepl.DeleteMessage:
		return p.processDeleteMessage(xld.WALStart, msg)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		batch.LastCheckPointID = int64(xld.WALStart)
	case *pglogrepl.RelationMessage:
		log.Infof("RelationMessage => RelationID: %d, Namespace: %s, RelationName: %s, Columns: %v",
			msg.RelationID, msg.Namespace, msg.RelationName, msg.Columns)
		if p.relationMessageMapping[msg.RelationID] == nil {
			_, ok := p.tableNameMapping[fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName)]
			// either it's a table we are aware of from SetupFlow, so it is in the map
			// or it is a table we are not aware of, in a schema we are aware of.
			if ok || slices.Contains(p.schemas, msg.Namespace) {
				p.relationMessageMapping[msg.RelationID] = convertRelationMessageToProto(msg)
				if !ok {
					if p.allowTableAdditions {
						// stop processing, return this to ensure new table is created
						return &model.AddedTableRecord{
							CheckPointID: int64(xld.WALStart),
							TableName:    msg.RelationName,
							SrcSchema:    msg.Namespace,
						}, nil
					} else {
						// the table is new, but we aren't going to add it to the destination.
						delete(p.relationMessageMapping, msg.RelationID)
					}
				}
			}
		} else {
			return p.processRelationMessage(xld.WALStart, convertRelationMessageToProto(msg))
		}

	case *pglogrepl.TruncateMessage:
		log.Warnf("TruncateMessage not supported")
	default:
		// Ignore other message types
		log.Warnf("Ignoring message type: %T", reflect.TypeOf(logicalMsg))
	}

	return nil, nil
}

func (p *postgresCDCSource) processInsertMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
) (model.Record, error) {
	tableName, exists := p.srcTableIDNameMapping[msg.RelationID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("InsertMessage => LSN: %d, RelationID: %d, Relation Name: %s", lsn, msg.RelationID, tableName)

	rel, ok := p.relationMessageMapping[msg.RelationID]
	if !ok {
		log.Warnf("unknown relation id: %d", msg.RelationID)
		return nil, nil
	}

	// create empty map of string to interface{}
	items, unchangedToastColumns, err := p.convertTupleToMap(msg.Tuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord{
		CheckPointID:          int64(lsn),
		Items:                 items,
		DestinationTableName:  p.tableNameMapping[tableName],
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func (p *postgresCDCSource) processUpdateMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
) (model.Record, error) {
	tableName, exists := p.srcTableIDNameMapping[msg.RelationID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("UpdateMessage => LSN: %d, RelationID: %d, Relation Name: %s", lsn, msg.RelationID, tableName)

	rel, ok := p.relationMessageMapping[msg.RelationID]
	if !ok {
		log.Warnf("unknown relation id: %d", msg.RelationID)
		return nil, nil
	}

	// create empty map of string to interface{}
	oldItems, _, err := p.convertTupleToMap(msg.OldTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, unchangedToastColumns, err := p.convertTupleToMap(msg.NewTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	return &model.UpdateRecord{
		CheckPointID:          int64(lsn),
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.tableNameMapping[tableName],
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

// processDeleteMessage processes a delete message and returns a DeleteRecord
func (p *postgresCDCSource) processDeleteMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
) (model.Record, error) {
	tableName, exists := p.srcTableIDNameMapping[msg.RelationID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("DeleteMessage => LSN: %d, RelationID: %d, Relation Name: %s", lsn, msg.RelationID, tableName)

	rel, ok := p.relationMessageMapping[msg.RelationID]
	if !ok {
		log.Warnf("unknown relation id: %d", msg.RelationID)
		return nil, nil
	}

	// create empty map of string to interface{}
	items, unchangedToastColumns, err := p.convertTupleToMap(msg.OldTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord{
		CheckPointID:          int64(lsn),
		Items:                 items,
		DestinationTableName:  p.tableNameMapping[tableName],
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

/*
convertTupleToMap converts a PostgreSQL logical replication
tuple to a map representation.
It takes a tuple and a relation message as input and returns
1. a map of column names to values and
2. a string slice of unchanged TOAST column names
*/
func (p *postgresCDCSource) convertTupleToMap(
	tuple *pglogrepl.TupleData,
	rel *protos.RelationMessage,
) (*model.RecordItems, map[string]bool, error) {
	// if the tuple is nil, return an empty map
	if tuple == nil {
		return model.NewRecordItems(), make(map[string]bool), nil
	}

	// create empty map of string to interface{}
	items := model.NewRecordItems()
	unchangedToastColumns := make(map[string]bool)

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			val := &qvalue.QValue{Kind: qvalue.QValueKindInvalid, Value: nil}
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
			unchangedToastColumns[colName] = true
		default:
			return nil, nil, fmt.Errorf("unknown column data type: %s", string(col.DataType))
		}
	}
	return items, unchangedToastColumns, nil
}

func (p *postgresCDCSource) decodeColumnData(data []byte, dataType uint32, formatCode int16) (*qvalue.QValue, error) {
	var parsedData any
	var err error
	if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		if dt.Name == "uuid" {
			// below is required to decode uuid to string
			parsedData, err = dt.Codec.DecodeDatabaseSQLValue(p.typeMap, dataType, pgtype.TextFormatCode, data)
		} else {
			parsedData, err = dt.Codec.DecodeValue(p.typeMap, dataType, formatCode, data)
		}
		if err != nil {
			return nil, err
		}
		retVal, err := parseFieldFromPostgresOID(dataType, parsedData)
		if err != nil {
			return nil, err
		}
		return retVal, nil
	} else if dataType == uint32(oid.T_timetz) { // ugly TIMETZ workaround for CDC decoding.
		retVal, err := parseFieldFromPostgresOID(dataType, string(data))
		if err != nil {
			return nil, err
		}
		return retVal, nil
	}
	return &qvalue.QValue{Kind: qvalue.QValueKindString, Value: string(data)}, nil
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

// processRelationMessage processes a delete message and returns a TableSchemaDelta
func (p *postgresCDCSource) processRelationMessage(
	lsn pglogrepl.LSN,
	currRel *protos.RelationMessage,
) (model.Record, error) {
	// retrieve initial RelationMessage for table changed.
	prevRel := p.relationMessageMapping[currRel.RelationId]
	// creating maps for lookup later
	prevRelMap := make(map[string]bool)
	currRelMap := make(map[string]bool)
	for _, column := range prevRel.Columns {
		prevRelMap[column.Name] = true
	}
	for _, column := range currRel.Columns {
		currRelMap[column.Name] = true
	}

	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName:   p.srcTableIDNameMapping[currRel.RelationId],
		DstTableName:   p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationId]],
		AddedColumns:   make([]*protos.DeltaAddedColumn, 0),
		DroppedColumns: make([]string, 0),
	}
	for _, column := range currRel.Columns {
		// not present in previous relation message, but in current one, so added.
		if !prevRelMap[column.Name] {
			schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.DeltaAddedColumn{
				ColumnName: column.Name,
				ColumnType: string(postgresOIDToQValueKind(column.DataType)),
			})
		}
	}
	for _, column := range prevRel.Columns {
		// present in previous relation message, but not in current one, so dropped.
		if !currRelMap[column.Name] {
			schemaDelta.DroppedColumns = append(schemaDelta.DroppedColumns, column.Name)
		}
	}

	p.relationMessageMapping[currRel.RelationId] = currRel
	return &model.RelationRecord{
		TableSchemaDelta: schemaDelta,
		CheckPointID:     int64(lsn),
	}, nil
}
