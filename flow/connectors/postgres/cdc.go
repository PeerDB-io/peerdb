package connpostgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq/oid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	geo "github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type TxBuffer struct {
	Streams      [][]byte
	Lsn          pglogrepl.LSN
	FirstSegment bool
}

type PostgresCDCSource struct {
	*PostgresConnector
	*PostgresCDCConfig
	typeMap    *pgtype.Map
	commitLock *pglogrepl.BeginMessage
	txBuffer   map[uint32]*TxBuffer
	inStream   bool
}

type PostgresCDCConfig struct {
	CatalogPool            *pgxpool.Pool
	SrcTableIDNameMapping  map[uint32]string
	TableNameMapping       map[string]model.NameAndExclude
	TableNameSchemaMapping map[string]*protos.TableSchema
	// for partitioned tables, maps child relid to parent relid
	ChildToParentRelIDMap map[uint32]uint32
	// for storing schema delta audit logs to catalog
	RelationMessageMapping model.RelationMessageMapping
	FlowJobName            string
	Slot                   string
	Publication            string
	Version                int32
}

// Create a new PostgresCDCSource
func (c *PostgresConnector) NewPostgresCDCSource(cdcConfig *PostgresCDCConfig) *PostgresCDCSource {
	var txBuffer map[uint32]*TxBuffer
	if cdcConfig.Version >= 2 {
		txBuffer = make(map[uint32]*TxBuffer)
	}
	return &PostgresCDCSource{
		PostgresConnector: c,
		PostgresCDCConfig: cdcConfig,
		typeMap:           pgtype.NewMap(),
		commitLock:        nil,
		inStream:          false,
		txBuffer:          txBuffer,
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

// replProcessor implements ingesting PostgreSQL logical replication tuples into items.
type replProcessor[Items model.Items] interface {
	NewItems(int) Items

	Process(
		items Items,
		p *PostgresCDCSource,
		tuple *pglogrepl.TupleDataColumn,
		col *pglogrepl.RelationMessageColumn,
	) error
}

type pgProcessor struct{}

func (pgProcessor) NewItems(size int) model.PgItems {
	return model.NewPgItems(size)
}

func (pgProcessor) Process(
	items model.PgItems,
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleDataColumn,
	col *pglogrepl.RelationMessageColumn,
) error {
	switch tuple.DataType {
	case 'n': // null
		items.AddColumn(col.Name, nil)
	case 't': // text
		// bytea also appears here as a hex
		items.AddColumn(col.Name, tuple.Data)
	case 'b': // binary
		return fmt.Errorf(
			"binary encoding not supported, received for %s type %d",
			col.Name,
			col.DataType,
		)
	default:
		return fmt.Errorf("unknown column data type: %s", string(tuple.DataType))
	}
	return nil
}

type qProcessor struct{}

func (qProcessor) NewItems(size int) model.RecordItems {
	return model.NewRecordItems(size)
}

func (qProcessor) Process(
	items model.RecordItems,
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleDataColumn,
	col *pglogrepl.RelationMessageColumn,
) error {
	switch tuple.DataType {
	case 'n': // null
		items.AddColumn(col.Name, qvalue.QValueNull(qvalue.QValueKindInvalid))
	case 't': // text
		// bytea also appears here as a hex
		data, err := p.decodeColumnData(tuple.Data, col.DataType, pgtype.TextFormatCode)
		if err != nil {
			p.logger.Error("error decoding text column data", slog.Any("error", err),
				slog.String("columnName", col.Name), slog.Int64("dataType", int64(col.DataType)))
			return fmt.Errorf("error decoding text column data: %w", err)
		}
		items.AddColumn(col.Name, data)
	case 'b': // binary
		data, err := p.decodeColumnData(tuple.Data, col.DataType, pgtype.BinaryFormatCode)
		if err != nil {
			return fmt.Errorf("error decoding binary column data: %w", err)
		}
		items.AddColumn(col.Name, data)
	default:
		return fmt.Errorf("unknown column data type: %s", string(tuple.DataType))
	}
	return nil
}

func processTuple[Items model.Items](
	processor replProcessor[Items],
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessage,
	exclude map[string]struct{},
) (Items, map[string]struct{}, error) {
	// if the tuple is nil, return an empty map
	if tuple == nil {
		return processor.NewItems(0), nil, nil
	}

	items := processor.NewItems(len(tuple.Columns))
	var unchangedToastColumns map[string]struct{}

	for idx, tcol := range tuple.Columns {
		rcol := rel.Columns[idx]
		if _, ok := exclude[rcol.Name]; ok {
			continue
		}
		if tcol.DataType == 'u' {
			if unchangedToastColumns == nil {
				unchangedToastColumns = make(map[string]struct{})
			}
			unchangedToastColumns[rcol.Name] = struct{}{}
		} else if err := processor.Process(items, p, tcol, rcol); err != nil {
			var none Items
			return none, nil, err
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

type cdcRecordProcessor[Items model.Items] struct {
	recordStore                *utils.CdcStore[Items]
	records                    *model.CDCStream[Items]
	pullRequest                *model.PullRecordsRequest[Items]
	processor                  replProcessor[Items]
	nextStandbyMessageDeadline time.Time
}

func (rp *cdcRecordProcessor[Items]) addRecordWithKey(logger log.Logger, key model.TableWithPkey, rec model.Record[Items]) error {
	if err := rp.recordStore.Set(logger, key, rec); err != nil {
		return err
	}
	rp.records.AddRecord(rec)

	if rp.recordStore.Len() == 1 {
		rp.records.SignalAsNotEmpty()
		rp.nextStandbyMessageDeadline = time.Now().Add(rp.pullRequest.IdleTimeout)
		logger.Info(fmt.Sprintf("pushing the standby deadline to %s", rp.nextStandbyMessageDeadline))
	}
	return nil
}

// PullCdcRecords pulls records from req's cdc stream
func PullCdcRecords[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
) error {
	logger := logger.LoggerFromCtx(ctx)
	conn := p.replConn.PgConn()
	records := req.RecordStream
	// clientXLogPos is the last checkpoint id, we need to ack that we have processed
	// until clientXLogPos each time we send a standby status update.
	var clientXLogPos pglogrepl.LSN
	if req.LastOffset > 0 {
		clientXLogPos = pglogrepl.LSN(req.LastOffset)

		err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(req.ConsumedOffset.Load())})
		if err != nil {
			return fmt.Errorf("[initial-flush] SendStandbyStatusUpdate failed: %w", err)
		}
	}

	var standByLastLogged time.Time
	cdcRecordStore, err := utils.NewCDCStore[Items](ctx, p.FlowJobName)
	if err != nil {
		return err
	}
	defer func() {
		if cdcRecordStore.IsEmpty() {
			records.SignalAsEmpty()
		}
		logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", cdcRecordStore.Len()))
		if err := cdcRecordStore.Close(); err != nil {
			logger.Warn("failed to clean up records storage", slog.Any("error", err))
		}
	}()

	shutdown := shared.Interval(ctx, time.Minute, func() {
		logger.Info(fmt.Sprintf("pulling records, currently have %d records", cdcRecordStore.Len()))
	})
	defer shutdown()

	standbyMessageTimeout := req.IdleTimeout

	recordProcessor := cdcRecordProcessor[Items]{
		recordStore:                cdcRecordStore,
		records:                    records,
		nextStandbyMessageDeadline: time.Now().Add(standbyMessageTimeout),
		pullRequest:                req,
		processor:                  processor,
	}

	pkmRequiresResponse := false
	waitingForCommit := false

	for {
		if pkmRequiresResponse {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(req.ConsumedOffset.Load())})
			if err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}
			pkmRequiresResponse = false

			if time.Since(standByLastLogged) > 10*time.Second {
				numRowsProcessedMessage := fmt.Sprintf("processed %d rows", cdcRecordStore.Len())
				logger.Info("Sent Standby status message. " + numRowsProcessedMessage)
				standByLastLogged = time.Now()
			}
		}

		if p.commitLock == nil {
			cdclen := cdcRecordStore.Len()
			if cdclen >= 0 && uint32(cdclen) >= req.MaxBatchSize {
				break
			}

			if waitingForCommit {
				logger.Info(fmt.Sprintf(
					"[%s] commit received, returning currently accumulated records - %d",
					p.FlowJobName,
					cdcRecordStore.Len()),
				)
				break
			}
		}

		// if we are past the next standby deadline (?)
		if time.Now().After(recordProcessor.nextStandbyMessageDeadline) {
			if !cdcRecordStore.IsEmpty() {
				logger.Info(fmt.Sprintf("standby deadline reached, have %d records", cdcRecordStore.Len()))

				if p.commitLock == nil {
					logger.Info(
						fmt.Sprintf("no commit lock, returning currently accumulated records - %d",
							cdcRecordStore.Len()))
					return nil
				} else {
					logger.Info(fmt.Sprintf("commit lock, waiting for commit to return records - %d",
						cdcRecordStore.Len()))
					waitingForCommit = true
				}
			} else {
				logger.Info(fmt.Sprintf("[%s] standby deadline reached, no records accumulated, continuing to wait",
					p.FlowJobName),
				)
			}
			recordProcessor.nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		var receiveCtx context.Context
		var cancel context.CancelFunc
		if cdcRecordStore.IsEmpty() {
			receiveCtx, cancel = context.WithCancel(ctx)
		} else {
			receiveCtx, cancel = context.WithDeadline(ctx, recordProcessor.nextStandbyMessageDeadline)
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
					cdcRecordStore.Len()))
				break
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
			if err := recordProcessor.processXLogData(ctx, p, xld, msg.Data[1:], clientXLogPos); err != nil {
				return fmt.Errorf("error processing message: %w", err)
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}

	for xid, txbuf := range p.txBuffer {
		if _, err := p.CatalogPool.Exec(
			ctx,
			"insert into v2cdc (flow_name, xid, lsn, stream) values ($1, $2, $3, $4) on conflict do nothing",
			p.FlowJobName,
			xid,
			txbuf.Lsn,
			txbuf.Streams,
		); err != nil {
			return err
		}
	}

	return nil
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

func (rp *cdcRecordProcessor[Items]) processXLogData(
	ctx context.Context,
	p *PostgresCDCSource,
	xld pglogrepl.XLogData,
	xldbytes []byte,
	currentClientXlogPos pglogrepl.LSN,
) error {
	var logicalMsg pglogrepl.Message
	var err error
	if p.Version < 2 {
		logicalMsg, err = pglogrepl.Parse(xld.WALData)
	} else {
		if p.inStream &&
			(xld.WALData[0] == byte(pglogrepl.MessageTypeUpdate) ||
				xld.WALData[0] == byte(pglogrepl.MessageTypeInsert) ||
				xld.WALData[0] == byte(pglogrepl.MessageTypeDelete) ||
				xld.WALData[0] == byte(pglogrepl.MessageTypeRelation)) {
			xid := binary.BigEndian.Uint32(xld.WALData[1:])
			txbuf := p.txBuffer[xid]
			txbuf.Streams = append(txbuf.Streams, xldbytes)
		} else {
			logicalMsg, err = pglogrepl.ParseV2(xld.WALData, p.inStream)
		}
	}
	if err != nil {
		return fmt.Errorf("error parsing logical message: %w", err)
	}
	return rp.processMessage(ctx, p, xld.WALStart, logicalMsg, currentClientXlogPos)
}

func (rp *cdcRecordProcessor[Items]) processMessage(
	ctx context.Context,
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	logicalMsg pglogrepl.Message,
	currentClientXlogPos pglogrepl.LSN,
) error {
	logger := logger.LoggerFromCtx(ctx)

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		logger.Debug("BeginMessage", slog.Any("FinalLSN", msg.FinalLSN), slog.Any("XID", msg.Xid))
		p.commitLock = msg
	case *pglogrepl.InsertMessage:
		return rp.processInsertMessage(p, lsn, msg)
	case *pglogrepl.InsertMessageV2:
		return rp.processInsertMessage(p, lsn, &msg.InsertMessage)
	case *pglogrepl.UpdateMessage:
		return rp.processUpdateMessage(p, lsn, msg)
	case *pglogrepl.UpdateMessageV2:
		return rp.processUpdateMessage(p, lsn, &msg.UpdateMessage)
	case *pglogrepl.DeleteMessage:
		return rp.processDeleteMessage(p, lsn, msg)
	case *pglogrepl.DeleteMessageV2:
		return rp.processDeleteMessage(p, lsn, &msg.DeleteMessage)
	case *pglogrepl.RelationMessage:
		return rp.processRelationMessage(ctx, p, currentClientXlogPos, msg)
	case *pglogrepl.RelationMessageV2:
		return rp.processRelationMessage(ctx, p, currentClientXlogPos, &msg.RelationMessage)
	case *pglogrepl.LogicalDecodingMessage:
		return rp.processLogicalDecodingMessage(p, lsn, msg)
	case *pglogrepl.LogicalDecodingMessageV2:
		return rp.processLogicalDecodingMessage(p, lsn, &msg.LogicalDecodingMessage)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		logger.Debug("CommitMessage", slog.Any("CommitLSN", msg.CommitLSN), slog.Any("TransactionEndLSN", msg.TransactionEndLSN))
		rp.records.UpdateLatestCheckpoint(int64(msg.CommitLSN))
		p.commitLock = nil
	case *pglogrepl.StreamCommitMessageV2:
		txbuf := p.txBuffer[msg.Xid]
		if !txbuf.FirstSegment {
			rows, err := p.CatalogPool.Query(ctx,
				"select stream from v2cdc where flow_name = $1 and xid = $2 order by lsn",
				p.FlowJobName, msg.Xid)
			if err != nil {
				return err
			}
			for rows.Next() {
				var stream [][]byte
				if err := rows.Scan(&stream); err != nil {
					return err
				}

				for _, m := range stream {
					mxld, err := pglogrepl.ParseXLogData(m)
					if err != nil {
						return err
					}
					logicalMsg, err = pglogrepl.ParseV2(mxld.WALData, p.inStream)
					if err != nil {
						return err
					}
					if err := rp.processMessage(ctx, p, mxld.WALStart, logicalMsg, currentClientXlogPos); err != nil {
						return err
					}
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}
		}

		for _, m := range txbuf.Streams {
			mxld, err := pglogrepl.ParseXLogData(m)
			if err != nil {
				return err
			}
			logicalMsg, err = pglogrepl.ParseV2(mxld.WALData, p.inStream)
			if err != nil {
				return err
			}
			if err := rp.processMessage(ctx, p, mxld.WALStart, logicalMsg, currentClientXlogPos); err != nil {
				return err
			}
		}
		rp.records.UpdateLatestCheckpoint(int64(msg.CommitLSN))
		delete(p.txBuffer, msg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		if txbuf, ok := p.txBuffer[msg.Xid]; ok && !txbuf.FirstSegment {
			if _, err := p.CatalogPool.Exec(ctx,
				"delete from v2cdc where flow_name = $1 and xid = $2",
				p.FlowJobName, msg.Xid,
			); err != nil {
				return err
			}
		}
		delete(p.txBuffer, msg.Xid)
	case *pglogrepl.StreamStartMessageV2:
		if _, ok := p.txBuffer[msg.Xid]; !ok {
			p.txBuffer[msg.Xid] = &TxBuffer{Lsn: lsn, FirstSegment: msg.FirstSegment != 0}
		}
		p.inStream = true
	case *pglogrepl.StreamStopMessageV2:
		p.inStream = false

	default:
		logger.Warn(fmt.Sprintf("%T not supported", msg))
	}

	return nil
}

func (rp *cdcRecordProcessor[Items]) processInsertMessage(
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
) error {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("InsertMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", relID)
	}

	items, _, err := processTuple(rp.processor, p, msg.Tuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return fmt.Errorf("error converting tuple to map: %w", err)
	}

	rec := &model.InsertRecord[Items]{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.TableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}
	isFullReplica := rp.pullRequest.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
	if isFullReplica {
		err := rp.addRecordWithKey(p.logger, model.TableWithPkey{}, rec)
		if err != nil {
			return err
		}
	} else {
		tablePkeyVal, err := model.RecToTablePKey(rp.pullRequest.TableNameSchemaMapping, rec)
		if err != nil {
			return err
		}

		err = rp.addRecordWithKey(p.logger, tablePkeyVal, rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rp *cdcRecordProcessor[Items]) processUpdateMessage(
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
) error {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("UpdateMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", relID)
	}

	oldItems, _, err := processTuple(rp.processor, p, msg.OldTuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, unchangedToastColumns, err := processTuple(
		rp.processor, p, msg.NewTuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return fmt.Errorf("error converting new tuple to map: %w", err)
	}

	rec := &model.UpdateRecord[Items]{
		BaseRecord:            p.baseRecord(lsn),
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.TableNameMapping[tableName].Name,
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}

	// tableName here is destination tableName.
	// should be ideally sourceTableName as we are in PullRecords.
	// will change in future
	isFullReplica := rp.pullRequest.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
	if isFullReplica {
		err := rp.addRecordWithKey(p.logger, model.TableWithPkey{}, rec)
		if err != nil {
			return err
		}
	} else {
		tablePkeyVal, err := model.RecToTablePKey(rp.pullRequest.TableNameSchemaMapping, rec)
		if err != nil {
			return err
		}

		latestRecord, ok, err := rp.recordStore.Get(tablePkeyVal)
		if err != nil {
			return err
		}
		if !ok {
			err = rp.addRecordWithKey(p.logger, tablePkeyVal, rec)
		} else {
			// iterate through unchanged toast cols and set them in new record
			updatedCols := rec.NewItems.UpdateIfNotExists(latestRecord.GetItems())
			for _, col := range updatedCols {
				delete(rec.UnchangedToastColumns, col)
			}
			err = rp.addRecordWithKey(p.logger, tablePkeyVal, rec)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (rp *cdcRecordProcessor[Items]) processDeleteMessage(
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
) error {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.SrcTableIDNameMapping[relID]
	if !exists {
		return nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug(fmt.Sprintf("DeleteMessage => LSN: %d, RelationID: %d, Relation Name: %s",
		lsn, relID, tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", relID)
	}

	items, _, err := processTuple(rp.processor, p, msg.OldTuple, rel, p.TableNameMapping[tableName].Exclude)
	if err != nil {
		return fmt.Errorf("error converting tuple to map: %w", err)
	}

	rec := &model.DeleteRecord[Items]{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.TableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}
	isFullReplica := rp.pullRequest.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
	if isFullReplica {
		if err := rp.addRecordWithKey(p.logger, model.TableWithPkey{}, rec); err != nil {
			return err
		}
	} else {
		tablePkeyVal, err := model.RecToTablePKey(rp.pullRequest.TableNameSchemaMapping, rec)
		if err != nil {
			return err
		}

		latestRecord, ok, err := rp.recordStore.Get(tablePkeyVal)
		if err != nil {
			return err
		}
		if ok {
			rec.Items = latestRecord.GetItems()
			if updateRecord, ok := latestRecord.(*model.UpdateRecord[Items]); ok {
				rec.UnchangedToastColumns = updateRecord.UnchangedToastColumns
			}
		} else {
			// there is nothing to backfill the items in the delete record with,
			// so don't update the row with this record
			// add sentinel value to prevent update statements from selecting
			rec.UnchangedToastColumns = map[string]struct{}{
				"_peerdb_not_backfilled_delete": {},
			}
		}

		// A delete can only be followed by an INSERT, which does not need backfilling
		// No need to store DeleteRecords in memory or disk.
		if err := rp.addRecordWithKey(p.logger, model.TableWithPkey{}, rec); err != nil {
			return err
		}
	}
	return nil
}

func auditSchemaDelta[Items model.Items](ctx context.Context, p *PostgresCDCSource, rec *model.RelationRecord[Items]) error {
	activityInfo := activity.GetInfo(ctx)
	workflowID := activityInfo.WorkflowExecution.ID
	runID := activityInfo.WorkflowExecution.RunID

	_, err := p.CatalogPool.Exec(ctx,
		`INSERT INTO peerdb_stats.schema_deltas_audit_log(flow_job_name,workflow_id,run_id,delta_info)
		 VALUES($1,$2,$3,$4)`,
		p.FlowJobName, workflowID, runID, rec)
	if err != nil {
		return fmt.Errorf("failed to insert row into table: %w", err)
	}
	return nil
}

func (rp *cdcRecordProcessor[Items]) processRelationMessage(
	ctx context.Context,
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.RelationMessage,
) error {
	// treat all relation messages as corresponding to parent if partitioned.
	msg.RelationID = p.getParentRelIDIfPartitioned(msg.RelationID)

	if _, exists := p.SrcTableIDNameMapping[msg.RelationID]; !exists {
		return nil
	}

	p.logger.Debug("RelationMessage",
		slog.Any("RelationID", msg.RelationID),
		slog.String("Namespace", msg.Namespace),
		slog.String("RelationName", msg.RelationName),
		slog.Any("Columns", msg.Columns))

	// not present in tables to sync, return immediately
	if _, ok := p.SrcTableIDNameMapping[msg.RelationID]; !ok {
		p.logger.Info("relid not present in srcTableIDNameMapping, skipping relation message",
			slog.Uint64("relId", uint64(msg.RelationID)))
		return nil
	}

	// retrieve current TableSchema for table changed
	// tableNameSchemaMapping uses dst table name as the key, so annoying lookup
	prevSchema := p.TableNameSchemaMapping[p.TableNameMapping[p.SrcTableIDNameMapping[msg.RelationID]].Name]
	// creating maps for lookup later
	prevRelMap := make(map[string]string)
	currRelMap := make(map[string]string)
	for _, column := range prevSchema.Columns {
		prevRelMap[column.Name] = column.Type
	}
	for _, column := range msg.Columns {
		switch prevSchema.System {
		case protos.TypeSystem_Q:
			qKind := p.postgresOIDToQValueKind(column.DataType)
			if qKind == qvalue.QValueKindInvalid {
				typeName, ok := p.customTypesMapping[column.DataType]
				if ok {
					qKind = customTypeToQKind(typeName)
				}
			}
			currRelMap[column.Name] = string(qKind)
		case protos.TypeSystem_PG:
			currRelMap[column.Name] = p.postgresOIDToName(column.DataType)
		default:
			panic(fmt.Sprintf("cannot process schema changes for unknown type system %s", prevSchema.System))
		}
	}

	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName: p.SrcTableIDNameMapping[msg.RelationID],
		DstTableName: p.TableNameMapping[p.SrcTableIDNameMapping[msg.RelationID]].Name,
		AddedColumns: nil,
		System:       prevSchema.System,
	}
	for _, column := range msg.Columns {
		// not present in previous relation message, but in current one, so added.
		if _, ok := prevRelMap[column.Name]; !ok {
			// only add to delta if not excluded
			if _, ok := p.TableNameMapping[p.SrcTableIDNameMapping[msg.RelationID]].Exclude[column.Name]; !ok {
				schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.FieldDescription{
					Name:         column.Name,
					Type:         currRelMap[column.Name],
					TypeModifier: column.TypeModifier,
				},
				)
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

	p.relationMessageMapping[msg.RelationID] = msg
	// only log audit if there is actionable delta
	if len(schemaDelta.AddedColumns) > 0 {
		rec := &model.RelationRecord[Items]{
			BaseRecord:       p.baseRecord(lsn),
			TableSchemaDelta: schemaDelta,
		}
		if len(schemaDelta.AddedColumns) > 0 {
			logger := logger.LoggerFromCtx(ctx)
			logger.Info(fmt.Sprintf("Detected schema change for table %s, addedColumns: %v",
				schemaDelta.SrcTableName, schemaDelta.AddedColumns))
			rp.records.AddSchemaDelta(rp.pullRequest.TableNameMapping, schemaDelta)
		}
		return auditSchemaDelta(ctx, p, rec)
	}
	return nil
}

func (rp *cdcRecordProcessor[Items]) processLogicalDecodingMessage(
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.LogicalDecodingMessage,
) error {
	p.logger.Info("LogicalDecodingMessage",
		slog.Bool("Transactional", msg.Transactional),
		slog.String("Prefix", msg.Prefix),
		slog.Int64("LSN", int64(msg.LSN)))
	if !msg.Transactional {
		rp.records.UpdateLatestCheckpoint(int64(msg.LSN))
	}
	return rp.addRecordWithKey(p.logger, model.TableWithPkey{}, &model.MessageRecord[Items]{
		BaseRecord: p.baseRecord(lsn),
		Prefix:     msg.Prefix,
		Content:    msg.Content,
	})
}

func (p *PostgresCDCSource) getParentRelIDIfPartitioned(relID uint32) uint32 {
	if parentRelID, ok := p.ChildToParentRelIDMap[relID]; ok {
		return parentRelID
	}
	return relID
}
