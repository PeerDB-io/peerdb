package connpostgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"go.temporal.io/sdk/log"

	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	geo "github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type PostgresCDCSource struct {
	*PostgresConnector
	srcTableIDNameMapping  map[uint32]string
	tableNameMapping       map[string]model.NameAndExclude
	tableNameSchemaMapping map[string]*protos.TableSchema
	relationMessageMapping model.RelationMessageMapping
	slot                   string
	publication            string
	commitLock             *pglogrepl.BeginMessage

	// for partitioned tables, maps child relid to parent relid
	childToParentRelIDMapping map[uint32]uint32

	// for storing schema delta audit logs to catalog
	catalogPool                              shared.CatalogPool
	otelManager                              *otel_metrics.OtelManager
	hushWarnUnhandledMessageType             map[pglogrepl.MessageType]struct{}
	hushWarnUnknownTableDetected             map[uint32]struct{}
	flowJobName                              string
	handleInheritanceForNonPartitionedTables bool
}

type PostgresCDCConfig struct {
	CatalogPool                              shared.CatalogPool
	OtelManager                              *otel_metrics.OtelManager
	SrcTableIDNameMapping                    map[uint32]string
	TableNameMapping                         map[string]model.NameAndExclude
	TableNameSchemaMapping                   map[string]*protos.TableSchema
	RelationMessageMapping                   model.RelationMessageMapping
	FlowJobName                              string
	Slot                                     string
	Publication                              string
	HandleInheritanceForNonPartitionedTables bool
}

// Create a new PostgresCDCSource
func (c *PostgresConnector) NewPostgresCDCSource(ctx context.Context, cdcConfig *PostgresCDCConfig) (*PostgresCDCSource, error) {
	childToParentRelIDMap, err := getChildToParentRelIDMap(ctx,
		c.conn, slices.Collect(maps.Keys(cdcConfig.SrcTableIDNameMapping)),
		cdcConfig.HandleInheritanceForNonPartitionedTables)
	if err != nil {
		return nil, fmt.Errorf("error getting child to parent relid map: %w", err)
	}

	return &PostgresCDCSource{
		PostgresConnector:                        c,
		srcTableIDNameMapping:                    cdcConfig.SrcTableIDNameMapping,
		tableNameMapping:                         cdcConfig.TableNameMapping,
		tableNameSchemaMapping:                   cdcConfig.TableNameSchemaMapping,
		relationMessageMapping:                   cdcConfig.RelationMessageMapping,
		slot:                                     cdcConfig.Slot,
		publication:                              cdcConfig.Publication,
		commitLock:                               nil,
		childToParentRelIDMapping:                childToParentRelIDMap,
		catalogPool:                              cdcConfig.CatalogPool,
		otelManager:                              cdcConfig.OtelManager,
		hushWarnUnhandledMessageType:             make(map[pglogrepl.MessageType]struct{}),
		hushWarnUnknownTableDetected:             make(map[uint32]struct{}),
		flowJobName:                              cdcConfig.FlowJobName,
		handleInheritanceForNonPartitionedTables: cdcConfig.HandleInheritanceForNonPartitionedTables,
	}, nil
}

func getChildToParentRelIDMap(ctx context.Context,
	conn *pgx.Conn, parentTableOIDs []uint32, handleInheritanceForNonPartitionedTables bool,
) (map[uint32]uint32, error) {
	relkinds := "'p'"
	if handleInheritanceForNonPartitionedTables {
		relkinds = "'p', 'r'"
	}

	query := fmt.Sprintf(`
		SELECT parent.oid AS parentrelid, child.oid AS childrelid
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		WHERE parent.relkind IN (%s) AND parent.oid=ANY($1);
	`, relkinds)

	rows, err := conn.Query(ctx, query, parentTableOIDs)
	if err != nil {
		return nil, fmt.Errorf("error querying for child to parent relid map: %w", err)
	}

	childToParentRelIDMap := make(map[uint32]uint32)
	var parentRelID, childRelID pgtype.Uint32
	if _, err := pgx.ForEachRow(rows, []any{&parentRelID, &childRelID}, func() error {
		childToParentRelIDMap[childRelID.Uint32] = parentRelID.Uint32
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error iterating over child to parent relid map: %w", err)
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
		customTypeMapping map[uint32]shared.CustomDataType,
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
	customTypeMapping map[uint32]shared.CustomDataType,
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
	customTypeMapping map[uint32]shared.CustomDataType,
) error {
	switch tuple.DataType {
	case 'n': // null
		items.AddColumn(col.Name, qvalue.QValueNull(qvalue.QValueKindInvalid))
	case 't': // text
		// bytea also appears here as a hex
		data, err := p.decodeColumnData(tuple.Data, col.DataType, pgtype.TextFormatCode, customTypeMapping)
		if err != nil {
			p.logger.Error("error decoding text column data", slog.Any("error", err),
				slog.String("columnName", col.Name), slog.Int64("dataType", int64(col.DataType)))
			return fmt.Errorf("error decoding text column data: %w", err)
		}
		items.AddColumn(col.Name, data)
	case 'b': // binary
		data, err := p.decodeColumnData(tuple.Data, col.DataType, pgtype.BinaryFormatCode, customTypeMapping)
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
	customTypeMapping map[uint32]shared.CustomDataType,
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
		} else if err := processor.Process(items, p, tcol, rcol, customTypeMapping); err != nil {
			var none Items
			return none, nil, err
		}
	}
	return items, unchangedToastColumns, nil
}

func (p *PostgresCDCSource) decodeColumnData(
	data []byte, dataType uint32, formatCode int16, customTypeMapping map[uint32]shared.CustomDataType,
) (qvalue.QValue, error) {
	var parsedData any
	var err error
	if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		dtOid := oid.Oid(dt.OID)
		if dtOid == oid.T_cidr || dtOid == oid.T_inet || dtOid == oid.T_macaddr || dtOid == oid.T_xml {
			// below is required to decode above types to string
			parsedData, err = dt.Codec.DecodeDatabaseSQLValue(p.typeMap, dataType, formatCode, data)
		} else {
			parsedData, err = dt.Codec.DecodeValue(p.typeMap, dataType, formatCode, data)
		}
		if err != nil {
			if dtOid == oid.T_time || dtOid == oid.T_timetz ||
				dtOid == oid.T_timestamp || dtOid == oid.T_timestamptz {
				// indicates year is more than 4 digits or something similar,
				// which you can insert into postgres,
				// but not representable by time.Time
				p.logger.Warn(fmt.Sprintf("Invalidated and hence nulled %s data: %s",
					dt.Name, string(data)))
				switch dtOid {
				case oid.T_time:
					return qvalue.QValueNull(qvalue.QValueKindTime), nil
				case oid.T_timetz:
					return qvalue.QValueNull(qvalue.QValueKindTimeTZ), nil
				case oid.T_timestamp:
					return qvalue.QValueNull(qvalue.QValueKindTimestamp), nil
				case oid.T_timestamptz:
					return qvalue.QValueNull(qvalue.QValueKindTimestampTZ), nil
				}
			}
			return nil, err
		}
		return p.parseFieldFromPostgresOID(dataType, parsedData, customTypeMapping)
	} else if dataType == uint32(oid.T_timetz) { // ugly TIMETZ workaround for CDC decoding.
		return p.parseFieldFromPostgresOID(dataType, string(data), customTypeMapping)
	} else if typeData, ok := customTypeMapping[dataType]; ok {
		customQKind := customTypeToQKind(typeData)
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
		case qvalue.QValueKindEnum:
			return qvalue.QValueEnum{Val: string(data)}, nil
		case qvalue.QValueKindArrayString:
			return qvalue.QValueArrayString{Val: shared.ParsePgArrayToStringSlice(data, typeData.Delim)}, nil
		case qvalue.QValueKindArrayEnum:
			return qvalue.QValueArrayEnum{Val: shared.ParsePgArrayToStringSlice(data, typeData.Delim)}, nil
		default:
			return nil, fmt.Errorf("unknown custom qkind: %s", customQKind)
		}
	}

	return qvalue.QValueString{Val: string(data)}, nil
}

// PullCdcRecords pulls records from req's cdc stream
func PullCdcRecords[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
	replLock *sync.Mutex,
) error {
	logger := internal.LoggerFromCtx(ctx)

	// use only with taking replLock
	conn := p.replConn.PgConn()
	sendStandbyAfterReplLock := func(updateType string) error {
		replLock.Lock()
		defer replLock.Unlock()
		err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(req.ConsumedOffset.Load())})
		if err != nil {
			return fmt.Errorf("[%s] SendStandbyStatusUpdate failed: %w", updateType, err)
		}
		return nil
	}

	records := req.RecordStream
	// clientXLogPos is the last checkpoint id, we need to ack that we have processed
	// until clientXLogPos each time we send a standby status update.
	var clientXLogPos pglogrepl.LSN
	if req.LastOffset.ID > 0 {
		clientXLogPos = pglogrepl.LSN(req.LastOffset.ID)
		if err := sendStandbyAfterReplLock("initial-flush"); err != nil {
			return err
		}
	}

	var standByLastLogged time.Time
	cdcRecordsStorage, err := utils.NewCDCStore[Items](ctx, req.Env, p.flowJobName)
	if err != nil {
		return err
	}
	defer func() {
		if cdcRecordsStorage.IsEmpty() {
			records.SignalAsEmpty()
		}
		logger.Info("[finished] PullRecords", slog.Int("records", cdcRecordsStorage.Len()))
		if err := cdcRecordsStorage.Close(); err != nil {
			logger.Warn("failed to clean up records storage", slog.Any("error", err))
		}
	}()

	shutdown := shared.Interval(ctx, time.Minute, func() {
		logger.Info("pulling records", slog.Int("records", cdcRecordsStorage.Len()))
	})
	defer shutdown()

	nextStandbyMessageDeadline := time.Now().Add(req.IdleTimeout)

	addRecordWithKey := func(key model.TableWithPkey, rec model.Record[Items]) error {
		if err := cdcRecordsStorage.Set(logger, key, rec); err != nil {
			return err
		}
		if err := records.AddRecord(ctx, rec); err != nil {
			return err
		}

		if cdcRecordsStorage.Len() == 1 {
			records.SignalAsNotEmpty()
			nextStandbyMessageDeadline = time.Now().Add(req.IdleTimeout)
			logger.Info(fmt.Sprintf("pushing the standby deadline to %s", nextStandbyMessageDeadline))
		}
		return nil
	}

	pkmRequiresResponse := false
	waitingForCommit := false

	pkmEmptyBatchThrottleThresholdSeconds, err := internal.PeerDBPKMEmptyBatchThrottleThresholdSeconds(ctx, req.Env)
	if err != nil {
		logger.Error("failed to get PeerDBPKMEmptyBatchThrottleThresholdSeconds", slog.Any("error", err))
	}
	lastEmptyBatchPkmSentTime := time.Now()
	for {
		if pkmRequiresResponse {
			if cdcRecordsStorage.IsEmpty() && int64(clientXLogPos) > req.ConsumedOffset.Load() {
				err := p.updateConsumedOffset(ctx, logger, req.FlowJobName, req.ConsumedOffset, clientXLogPos)
				if err != nil {
					return err
				}
				lastEmptyBatchPkmSentTime = time.Now()
			}

			if err := sendStandbyAfterReplLock("pkm-response"); err != nil {
				return err
			}
			pkmRequiresResponse = false

			if time.Since(standByLastLogged) > 10*time.Second {
				logger.Info("Sent Standby status message",
					slog.Int("records", cdcRecordsStorage.Len()))
				standByLastLogged = time.Now()
			}
		}

		if p.commitLock == nil {
			if cdcRecordsStorage.Len() >= int(req.MaxBatchSize) {
				return nil
			}

			if waitingForCommit {
				logger.Info("commit received, returning currently accumulated records",
					slog.Int("records", cdcRecordsStorage.Len()))
				return nil
			}
		}

		// if we are past the next standby deadline (?)
		if time.Now().After(nextStandbyMessageDeadline) {
			if !cdcRecordsStorage.IsEmpty() {
				logger.Info("standby deadline reached", slog.Int("records", cdcRecordsStorage.Len()))

				if p.commitLock == nil {
					logger.Info("no commit lock, returning currently accumulated records",
						slog.Int("records", cdcRecordsStorage.Len()))
					return nil
				} else {
					logger.Info("commit lock, waiting for commit to return records",
						slog.Int("records", cdcRecordsStorage.Len()))
					waitingForCommit = true
				}
			} else {
				logger.Info(("standby deadline reached, no records accumulated, continuing to wait"))
			}
			nextStandbyMessageDeadline = time.Now().Add(req.IdleTimeout)
		}

		var receiveCtx context.Context
		var cancel context.CancelFunc
		if cdcRecordsStorage.IsEmpty() {
			receiveCtx, cancel = context.WithCancel(ctx)
		} else {
			receiveCtx, cancel = context.WithDeadline(ctx, nextStandbyMessageDeadline)
		}
		rawMsg, err := func() (pgproto3.BackendMessage, error) {
			replLock.Lock()
			defer replLock.Unlock()
			return conn.ReceiveMessage(receiveCtx)
		}()
		cancel()

		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("consumeStream preempted: %w", ctxErr)
		}

		if err != nil && p.commitLock == nil {
			if pgconn.Timeout(err) {
				logger.Info("Stand-by deadline reached, returning currently accumulated records",
					slog.Int("records", cdcRecordsStorage.Len()))
				return nil
			} else {
				return fmt.Errorf("ReceiveMessage failed: %w", err)
			}
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			return shared.LogError(logger, exceptions.NewPostgresWalError(errors.New("received error response"), msg))
		case *pgproto3.CopyData:
			p.otelManager.Metrics.FetchedBytesCounter.Add(ctx, int64(len(msg.Data)))
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
				}

				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}

				if pkm.ReplyRequested || (pkmEmptyBatchThrottleThresholdSeconds != -1 &&
					time.Since(lastEmptyBatchPkmSentTime) >= time.Duration(pkmEmptyBatchThrottleThresholdSeconds)*time.Second) {
					pkmRequiresResponse = true
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParseXLogData failed: %w", err)
				}

				logger.Debug("XLogData",
					slog.Any("WALStart", xld.WALStart), slog.Any("ServerWALEnd", xld.ServerWALEnd), slog.Any("ServerTime", xld.ServerTime))
				rec, err := processMessage(ctx, p, records, xld, clientXLogPos, processor)
				if err != nil {
					return fmt.Errorf("error processing message: %w", err)
				}

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				if rec != nil {
					tableName := rec.GetDestinationTableName()
					switch r := rec.(type) {
					case *model.UpdateRecord[Items]:
						// tableName here is destination tableName.
						// should be ideally sourceTableName as we are in PullRecords.
						// will change in future
						// TODO: replident is cached here, should not cache since it can change
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							latestRecord, ok, err := cdcRecordsStorage.Get(tablePkeyVal)
							if err != nil {
								return err
							}
							if ok {
								// iterate through unchanged toast cols and set them in new record
								updatedCols := r.NewItems.UpdateIfNotExists(latestRecord.GetItems())
								for _, col := range updatedCols {
									delete(r.UnchangedToastColumns, col)
								}
							}
							if err := addRecordWithKey(tablePkeyVal, rec); err != nil {
								return err
							}
						}

					case *model.InsertRecord[Items]:
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							if err := addRecordWithKey(tablePkeyVal, rec); err != nil {
								return err
							}
						}
					case *model.DeleteRecord[Items]:
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							latestRecord, ok, err := cdcRecordsStorage.Get(tablePkeyVal)
							if err != nil {
								return err
							}
							if ok {
								r.Items = latestRecord.GetItems()
								if updateRecord, ok := latestRecord.(*model.UpdateRecord[Items]); ok {
									r.UnchangedToastColumns = updateRecord.UnchangedToastColumns
								}
							} else {
								// there is nothing to backfill the items in the delete record with,
								// so don't update the row with this record
								// add sentinel value to prevent update statements from selecting
								r.UnchangedToastColumns = map[string]struct{}{
									"_peerdb_not_backfilled_delete": {},
								}
							}

							// A delete can only be followed by an INSERT, which does not need backfilling
							// No need to store DeleteRecords in memory or disk.
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						}

					case *model.RelationRecord[Items]:
						tableSchemaDelta := r.TableSchemaDelta
						if len(tableSchemaDelta.AddedColumns) > 0 {
							logger.Info(fmt.Sprintf("Detected schema change for table %s, addedColumns: %v",
								tableSchemaDelta.SrcTableName, tableSchemaDelta.AddedColumns))
							records.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
						}

					case *model.MessageRecord[Items]:
						// if cdc store empty, we can move lsn,
						// otherwise push to records so destination can ack once all previous messages processed
						if cdcRecordsStorage.IsEmpty() {
							if int64(clientXLogPos) > req.ConsumedOffset.Load() {
								err := p.updateConsumedOffset(ctx, logger, req.FlowJobName, req.ConsumedOffset, clientXLogPos)
								if err != nil {
									return err
								}
							}
						} else if err := records.AddRecord(ctx, rec); err != nil {
							return err
						}
					}
				}
			}
		}
	}
}

func (p *PostgresCDCSource) updateConsumedOffset(
	ctx context.Context,
	logger log.Logger,
	flowJobName string,
	consumedOffset *atomic.Int64,
	clientXLogPos pglogrepl.LSN,
) error {
	metadata := connmetadata.NewPostgresMetadataFromCatalog(logger, p.catalogPool)
	if err := metadata.SetLastOffset(ctx, flowJobName, model.CdcCheckpoint{ID: int64(clientXLogPos)}); err != nil {
		return err
	}
	consumedOffset.Store(int64(clientXLogPos))
	p.otelManager.Metrics.CommittedLSNGauge.Record(ctx, int64(clientXLogPos))
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

func processMessage[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	batch *model.CDCStream[Items],
	xld pglogrepl.XLogData,
	currentClientXlogPos pglogrepl.LSN,
	processor replProcessor[Items],
) (model.Record[Items], error) {
	logger := internal.LoggerFromCtx(ctx)
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}
	customTypeMapping, err := p.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		logger.Debug("BeginMessage", slog.Any("FinalLSN", msg.FinalLSN), slog.Any("XID", msg.Xid))
		p.commitLock = msg
	case *pglogrepl.InsertMessage:
		return processInsertMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.UpdateMessage:
		return processUpdateMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.DeleteMessage:
		return processDeleteMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		logger.Debug("CommitMessage",
			slog.Any("CommitLSN", msg.CommitLSN),
			slog.Any("TransactionEndLSN", msg.TransactionEndLSN))
		batch.UpdateLatestCheckpointID(int64(msg.CommitLSN))
		p.commitLock = nil
	case *pglogrepl.RelationMessage:
		// treat all relation messages as corresponding to parent if partitioned.
		msg.RelationID, err = p.checkIfUnknownTableInherits(ctx, msg.RelationID)
		if err != nil {
			return nil, err
		}

		if _, exists := p.srcTableIDNameMapping[msg.RelationID]; !exists {
			return nil, nil
		}

		logger.Debug("RelationMessage",
			slog.Any("RelationID", msg.RelationID),
			slog.String("Namespace", msg.Namespace),
			slog.String("RelationName", msg.RelationName),
			slog.Any("Columns", msg.Columns))

		return processRelationMessage[Items](ctx, p, currentClientXlogPos, msg)
	case *pglogrepl.LogicalDecodingMessage:
		logger.Info("LogicalDecodingMessage",
			slog.Bool("Transactional", msg.Transactional),
			slog.String("Prefix", msg.Prefix),
			slog.String("LSN", msg.LSN.String()))
		if !msg.Transactional {
			batch.UpdateLatestCheckpointID(int64(msg.LSN))
		}
		return &model.MessageRecord[Items]{
			BaseRecord: p.baseRecord(msg.LSN),
			Prefix:     msg.Prefix,
			Content:    string(msg.Content),
		}, nil

	default:
		if _, ok := p.hushWarnUnhandledMessageType[msg.Type()]; !ok {
			logger.Warn(fmt.Sprintf("Unhandled message type: %T", msg))
			p.hushWarnUnhandledMessageType[msg.Type()] = struct{}{}
		}
	}

	return nil, nil
}

func processInsertMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("InsertMessage", slog.Any("LSN", lsn), slog.Any("RelationID", relID), slog.String("Relation Name", tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	items, _, err := processTuple(processor, p, msg.Tuple, rel, p.tableNameMapping[tableName].Exclude, customTypeMapping)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord[Items]{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func processUpdateMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("UpdateMessage", slog.Any("LSN", lsn), slog.Any("RelationID", relID), slog.String("Relation Name", tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	oldItems, _, err := processTuple(processor, p, msg.OldTuple, rel,
		p.tableNameMapping[tableName].Exclude, customTypeMapping)
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	newItems, unchangedToastColumns, err := processTuple(
		processor, p, msg.NewTuple, rel, p.tableNameMapping[tableName].Exclude, customTypeMapping)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	/*
	   Looks like in some cases (at least replident full + TOAST), the new tuple doesn't contain unchanged columns
	   and only the old tuple does. So we can backfill the new tuple with the unchanged columns from the old tuple.
	   Otherwise, _peerdb_unchanged_toast_columns is set correctly and we fallback to normal unchanged TOAST handling in normalize,
	   but this doesn't work in connectors where we don't do unchanged TOAST handling in normalize.
	*/
	backfilledCols := newItems.UpdateIfNotExists(oldItems)
	for _, col := range backfilledCols {
		delete(unchangedToastColumns, col)
		// we only use _peerdb_data anyway, remove for space optimization
		oldItems.DeleteColName(col)
	}

	return &model.UpdateRecord[Items]{
		BaseRecord:            p.baseRecord(lsn),
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.tableNameMapping[tableName].Name,
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

// processDeleteMessage processes a delete message and returns a DeleteRecord
func processDeleteMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	relID := p.getParentRelIDIfPartitioned(msg.RelationID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("DeleteMessage", slog.Any("LSN", lsn), slog.Any("RelationID", relID), slog.String("Relation Name", tableName))

	rel, ok := p.relationMessageMapping[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", relID)
	}

	items, _, err := processTuple(processor, p, msg.OldTuple, rel,
		p.tableNameMapping[tableName].Exclude, customTypeMapping)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord[Items]{
		BaseRecord:           p.baseRecord(lsn),
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processRelationMessage processes a RelationMessage and returns a TableSchemaDelta
func processRelationMessage[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	currRel *pglogrepl.RelationMessage,
) (model.Record[Items], error) {
	// not present in tables to sync, return immediately
	currRelName, ok := p.srcTableIDNameMapping[currRel.RelationID]
	if !ok {
		p.logger.Warn("relid not present in srcTableIDNameMapping, skipping relation message",
			slog.Uint64("relId", uint64(currRel.RelationID)))
		return nil, nil
	}
	customTypeMapping, err := p.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	p.logger.Info("processing RelationMessage",
		slog.Any("LSN", lsn),
		slog.Any("RelationName", currRelName))
	// retrieve current TableSchema for table changed, mapping uses dst table name as key, need to translate source name
	currRelDstInfo, ok := p.tableNameMapping[currRelName]
	if !ok {
		p.logger.Error("Detected relation message for table, but not in table name mapping",
			slog.String("tableName", currRelName))
		return nil, fmt.Errorf("cannot find table name mapping for %s", currRelName)
	}

	prevSchema, ok := p.tableNameSchemaMapping[currRelDstInfo.Name]
	if !ok {
		p.logger.Error("Detected relation message for table, but not in table schema mapping",
			slog.String("tableName", currRelDstInfo.Name))
		return nil, fmt.Errorf("cannot find table schema for %s", currRelDstInfo.Name)
	}

	prevRelMap := make(map[string]string, len(prevSchema.Columns))
	for _, column := range prevSchema.Columns {
		prevRelMap[column.Name] = column.Type
	}

	currRelMap := make(map[string]string, len(currRel.Columns))
	for _, column := range currRel.Columns {
		switch prevSchema.System {
		case protos.TypeSystem_Q:
			qKind := p.postgresOIDToQValueKind(column.DataType, customTypeMapping)
			if qKind == qvalue.QValueKindInvalid {
				typeName, ok := customTypeMapping[column.DataType]
				if ok {
					qKind = customTypeToQKind(typeName)
				}
			}
			currRelMap[column.Name] = string(qKind)
		case protos.TypeSystem_PG:
			typeName, err := p.postgresOIDToName(column.DataType, customTypeMapping)
			if err != nil {
				return nil, err
			}
			currRelMap[column.Name] = typeName
		default:
			panic(fmt.Sprintf("cannot process schema changes for unknown type system %s", prevSchema.System))
		}
	}

	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName:    p.srcTableIDNameMapping[currRel.RelationID],
		DstTableName:    p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationID]].Name,
		AddedColumns:    nil,
		System:          prevSchema.System,
		NullableEnabled: prevSchema.NullableEnabled,
	}
	for _, column := range currRel.Columns {
		// not present in previous relation message, but in current one, so added.
		if _, ok := prevRelMap[column.Name]; !ok {
			// only add to delta if not excluded
			if _, ok := p.tableNameMapping[p.srcTableIDNameMapping[currRel.RelationID]].Exclude[column.Name]; !ok {
				schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.FieldDescription{
					Name:         column.Name,
					Type:         currRelMap[column.Name],
					TypeModifier: column.TypeModifier,
					Nullable:     column.Flags == 0, // pg does not send nullable info, only whether column is part of primary key
				})
				p.logger.Info("Detected added column",
					slog.String("columnName", column.Name),
					slog.String("columnType", currRelMap[column.Name]),
					slog.String("relationName", schemaDelta.SrcTableName))
			} else {
				p.logger.Warn(fmt.Sprintf("Detected added column %s in table %s, but not propagating because excluded",
					column.Name, schemaDelta.SrcTableName))
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
		rec := &model.RelationRecord[Items]{
			BaseRecord:       p.baseRecord(lsn),
			TableSchemaDelta: schemaDelta,
		}
		return rec, monitoring.AuditSchemaDelta(ctx, p.catalogPool.Pool, p.flowJobName, schemaDelta)
	}
	return nil, nil
}

// getParentRelIDIfPartitioned checks if the relation ID is a child table
// and returns the parent relation ID if it is.
// If the relation ID is not a child table, it returns the original relation ID.
// It also logs if the child table is detected for the first time.
func (p *PostgresCDCSource) getParentRelIDIfPartitioned(relID uint32) uint32 {
	parentRelID, ok := p.childToParentRelIDMapping[relID]
	if ok {
		if _, ok := p.hushWarnUnknownTableDetected[relID]; !ok {
			p.logger.Info("Detected child table in CDC stream, remapping to parent table",
				slog.Uint64("childRelID", uint64(relID)),
				slog.Uint64("parentRelID", uint64(parentRelID)),
				slog.String("parentTableName", p.srcTableIDNameMapping[parentRelID]))
			p.hushWarnUnknownTableDetected[relID] = struct{}{}
		}
		return parentRelID
	}

	return relID
}

// since we generate the childToParent mapping at the beginning of the CDC stream
// some child tables could be created after the CDC stream starts
// and we need to check if they inherit from a known table
// filtered by relkind; parent needs to be a partitioned table by default
func (p *PostgresCDCSource) checkIfUnknownTableInherits(ctx context.Context,
	relID uint32,
) (uint32, error) {
	relID = p.getParentRelIDIfPartitioned(relID)
	relkinds := "'p'"
	if p.handleInheritanceForNonPartitionedTables {
		relkinds = "'p', 'r'"
	}

	if _, ok := p.srcTableIDNameMapping[relID]; !ok {
		var parentRelID uint32
		err := p.conn.QueryRow(
			ctx,
			fmt.Sprintf(`SELECT inhparent FROM pg_inherits
			JOIN pg_class c ON pg_inherits.inhparent=c.oid
			WHERE inhrelid=$1 AND c.relkind IN (%s)`, relkinds),
			relID).Scan(&parentRelID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return relID, nil
			}
			return 0, fmt.Errorf("failed to query pg_inherits: %w", err)
		}
		p.childToParentRelIDMapping[relID] = parentRelID
		p.hushWarnUnknownTableDetected[relID] = struct{}{}
		p.logger.Info("Detected new child table in CDC stream, remapping to parent table",
			slog.Uint64("childRelID", uint64(relID)),
			slog.Uint64("parentRelID", uint64(parentRelID)),
			slog.String("parentTableName", p.srcTableIDNameMapping[parentRelID]))
		return parentRelID, nil
	}

	return relID, nil
}
