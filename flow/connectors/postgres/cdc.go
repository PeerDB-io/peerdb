package connpostgres

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

type PostgresCDCSource struct {
	ctx         context.Context
	conn        *pgxpool.Pool
	relID       uint32
	slot        string
	publication string
	relations   map[uint32]*pglogrepl.RelationMessage
	typeMap     *pgtype.Map
	startLSN    pglogrepl.LSN
}

type PostrgesCDCConfig struct {
	AppContext  context.Context
	Connection  *pgxpool.Pool
	Slot        string
	Publication string
	RelID       uint32
}

// Create a new PostgresCDCSource
func NewPostgresCDCSource(cdcConfing *PostrgesCDCConfig) (*PostgresCDCSource, error) {
	return &PostgresCDCSource{
		ctx:         cdcConfing.AppContext,
		conn:        cdcConfing.Connection,
		relID:       cdcConfing.RelID,
		slot:        cdcConfing.Slot,
		publication: cdcConfing.Publication,
		relations:   make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:     pgtype.NewMap(),
	}, nil
}

// Close closes the connection to the database.
func (p *PostgresCDCSource) Close() error {
	p.conn.Close()
	return nil
}

// PullRecords pulls records from the cdc stream
func (p *PostgresCDCSource) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	// setup options
	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", p.publication),
	}
	replicationOpts := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}

	// create replication connection
	replicationConn, err := p.conn.Acquire(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("error acquiring connection for replication: %w", err)
	}

	defer replicationConn.Release()

	pgConn := replicationConn.Conn().PgConn()
	log.Infof("created replication connection")

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

	err = pglogrepl.StartReplication(p.ctx, pgConn, p.slot, p.startLSN, replicationOpts)
	if err != nil {
		return nil, fmt.Errorf("error starting replication at startLsn - %d: %w", p.startLSN, err)
	}
	log.Infof("started replication on slot %s at startLSN: %d", p.slot, p.startLSN)

	return p.consumeStream(pgConn, req, p.startLSN)
}

// start consuming the cdc stream
func (p *PostgresCDCSource) consumeStream(
	conn *pgconn.PgConn,
	req *model.PullRecordsRequest,
	clientXLogPos pglogrepl.LSN,
) (*model.RecordBatch, error) {
	// TODO (kaushik): take into consideration the MaxBatchSize
	// parameters in the original request.
	result := &model.RecordBatch{
		Records: make([]model.Record, 0),
	}

	standbyMessageTimeout := req.IdleTimeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(p.ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return nil, fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}
			log.Debugf("Sent Standby status message")
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

			log.Debugf(
				"Primary Keepalive Message => ServerWALEnd: %s ServerTime: %s ReplyRequested: %t",
				pkm.ServerWALEnd,
				pkm.ServerTime,
				pkm.ReplyRequested,
			)

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
			rec, err := p.processMessage(xld)

			if err != nil {
				return nil, fmt.Errorf("error processing message: %w", err)
			}

			if !firstProcessed {
				firstProcessed = true
				result.FirstCheckPointID = int64(xld.WALStart)
			}

			if rec != nil {
				result.Records = append(result.Records, rec)
			}

			result.LastCheckPointID = int64(xld.WALStart)

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

func (p *PostgresCDCSource) processMessage(xld pglogrepl.XLogData) (model.Record, error) {
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
		log.Debugf("Ignoring CommitMessage")
	case *pglogrepl.RelationMessage:
		// TODO (kaushik): consider persistent state for a mirror job
		// to be stored somewhere in temporal state. We might need to persist
		// the state of the relation message somewhere
		log.Infof("RelationMessage => RelationID: %d, Namespace: %s, RelationName: %s, Columns: %v",
			msg.RelationID, msg.Namespace, msg.RelationName, msg.Columns)
		p.relations[msg.RelationID] = msg
	case *pglogrepl.TruncateMessage:
		log.Warnf("TruncateMessage not supported")
	default:
		// Ignore other message types
		log.Warnf("Ignoring message type: %T", reflect.TypeOf(logicalMsg))
	}

	return nil, nil
}

func (p *PostgresCDCSource) processInsertMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
) (model.Record, error) {
	if msg.RelationID != p.relID {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("InsertMessage => LSN: %d, RelationID: %d", lsn, msg.RelationID)

	// TODO look at why we are even getting messages with LSN less than the startLSN
	if lsn < p.startLSN {
		// log that we are skipping this message
		log.Warnf("Skipping message with LSN %d as it is less than minLSN %d", lsn, p.startLSN)
		return nil, nil
	}

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}

	// create empty map of string to interface{}
	items, err := p.convertTupleToMap(msg.Tuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord{
		CheckPointID: int64(lsn),
		Items:        items,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func (p *PostgresCDCSource) processUpdateMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
) (model.Record, error) {
	if msg.RelationID != p.relID {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("UpdateMessage => LSN: %d, RelationID: %d", lsn, msg.RelationID)

	// TODO look at why we are even getting messages with LSN less than the startLSN
	if lsn < p.startLSN {
		// log that we are skipping this message
		log.Warnf("Skipping message with LSN %d as it is less than minLSN %d", lsn, p.startLSN)
		return nil, nil
	}

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}

	// create empty map of string to interface{}
	oldItems, err := p.convertTupleToMap(msg.OldTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, err := p.convertTupleToMap(msg.NewTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	return &model.UpdateRecord{
		CheckPointID: int64(lsn),
		OldItems:     oldItems,
		NewItems:     newItems,
	}, nil
}

// processDeleteMessage processes a delete message and returns a DeleteRecord
func (p *PostgresCDCSource) processDeleteMessage(
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
) (model.Record, error) {
	if msg.RelationID != p.relID {
		return nil, nil
	}

	// log lsn and relation id for debugging
	log.Debugf("DeleteMessage => LSN: %d, RelationID: %d", lsn, msg.RelationID)

	// TODO look at why we are even getting messages with LSN less than the startLSN
	if lsn < p.startLSN {
		// log that we are skipping this message
		log.Warnf("Skipping message with LSN %d as it is less than minLSN %d", lsn, p.startLSN)
		return nil, nil
	}

	rel, ok := p.relations[msg.RelationID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", msg.RelationID)
	}

	// create empty map of string to interface{}
	items, err := p.convertTupleToMap(msg.OldTuple, rel)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord{
		CheckPointID: int64(lsn),
		Items:        items,
	}, nil
}

// convertTupleToMap converts a tuple to a map of column name to value
func (p *PostgresCDCSource) convertTupleToMap(
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessage,
) (map[string]interface{}, error) {
	// if the tuple is nil, return an empty map
	if tuple == nil {
		return make(map[string]interface{}), nil
	}

	// create empty map of string to interface{}
	items := make(map[string]interface{})
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			items[colName] = nil
		case 't': // text
			data, err := p.decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding text column data: %w", err)
			}
			items[colName] = data
		case 'b': // binary
			data, err := p.decodeBinaryColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding binary column data: %w", err)
			}
			items[colName] = data
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple,
			// and logical replication doesn't want to spend a disk read to fetch its value for you.
			// Instead, it sends a placeholder value of the form 'u<toast_oid>' and you are expected
			// to fetch the value yourself.

			// TODO (kaushik): support TOAST values
		default:
			return nil, fmt.Errorf("unknown column data type: %s", string(col.DataType))
		}
	}

	return items, nil
}

func (p *PostgresCDCSource) decodeTextColumnData(
	data []byte,
	dataType uint32,
) (interface{}, error) {
	if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(p.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// decodeBinaryColumnData decodes the binary data for a column
func (p *PostgresCDCSource) decodeBinaryColumnData(
	data []byte,
	dataType uint32,
) (interface{}, error) {
	if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(p.typeMap, dataType, pgtype.BinaryFormatCode, data)
	}
	return string(data), nil
}
