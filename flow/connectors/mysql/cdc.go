package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	system protos.TypeSystem,
	tableIdentifiers []string,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableIdentifiers))
	for _, tableName := range tableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tableName, system)
		if err != nil {
			c.logger.Info("error fetching schema for table "+tableName, slog.Any("error", err))
			return nil, err
		}
		res[tableName] = tableSchema
		c.logger.Info("fetched schema for table", slog.String("table", tableName))
	}

	return res, nil
}

func (c *MySqlConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tableName string,
	system protos.TypeSystem,
) (*protos.TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, err
	}

	nullableEnabled, err := peerdbenv.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	rs, err := c.Execute(ctx, fmt.Sprintf("select * from %s limit 0", schemaTable.MySQL()))
	if err != nil {
		return nil, err
	}
	columns := make([]*protos.FieldDescription, 0, len(rs.Values))
	primary := make([]string, 0)

	for _, field := range rs.Fields {
		qkind, err := qkindFromMysql(field)
		if err != nil {
			return nil, err
		}

		column := &protos.FieldDescription{
			Name:         string(field.Name),
			Type:         string(qkind),
			TypeModifier: 0, // TODO numeric precision info
			Nullable:     (field.Flag & mysql.NOT_NULL_FLAG) == 0,
		}
		if (field.Flag & mysql.PRI_KEY_FLAG) != 0 {
			primary = append(primary, column.Name)
		}
		columns = append(columns, column)
	}

	return &protos.TableSchema{
		TableIdentifier:       tableName,
		PrimaryKeyColumns:     primary,
		IsReplicaIdentityFull: false,
		System:                system,
		NullableEnabled:       nullableEnabled,
		Columns:               columns,
	}, nil
}

func (c *MySqlConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

func (c *MySqlConnector) ExportTxSnapshot(context.Context) (*protos.ExportTxSnapshotOutput, any, error) {
	// https://dev.mysql.com/doc/refman/8.4/en/replication-howto-masterstatus.html
	return nil, nil, nil
}

func (c *MySqlConnector) FinishExport(any) error {
	return nil
}

func (c *MySqlConnector) SetupReplication(
	ctx context.Context,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	gtidModeOn, err := c.GetGtidModeOn(ctx)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to get gtid_mode: %w", err)
	}
	var lastOffsetText string
	if gtidModeOn {
		set, err := c.GetMasterGTIDSet(ctx)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to GetMasterGTIDSet: %w", err)
		}
		lastOffsetText = set.String()
	} else {
		pos, err := c.GetMasterPos(ctx)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to GetMasterPos: %w", err)
		}
		lastOffsetText = fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos)
	}
	if err := c.SetLastOffset(
		ctx, req.FlowJobName, model.CdcCheckpoint{Text: lastOffsetText},
	); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to SetLastOffset: %w", err)
	}

	return model.SetupReplicationResult{}, nil
}

func (c *MySqlConnector) SetupReplConn(ctx context.Context) error {
	// mysql code will spin up new connection for each normalize for now
	return nil
}

func (c *MySqlConnector) startSyncer() *replication.BinlogSyncer {
	//nolint:gosec
	return replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   rand.Uint32(),
		Flavor:     c.Flavor(),
		Host:       c.config.Host,
		Port:       uint16(c.config.Port),
		User:       c.config.User,
		Password:   c.config.Password,
		Logger:     BinlogLogger{Logger: c.logger},
		UseDecimal: true,
		ParseTime:  true,
	})
}

func (c *MySqlConnector) startStreaming(
	pos string,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	if rest, isFile := strings.CutPrefix(pos, "!f:"); isFile {
		comma := strings.LastIndexByte(rest, ',')
		if comma == -1 {
			return nil, nil, nil, mysql.Position{}, fmt.Errorf("no comma in file/pos offset %s", pos)
		}
		offset, err := strconv.ParseUint(rest[comma+1:], 16, 32)
		if err != nil {
			return nil, nil, nil, mysql.Position{}, fmt.Errorf("invalid offset in file/pos offset %s: %w", pos, err)
		}
		return c.startCdcStreamingFilePos(mysql.Position{Name: rest[:comma], Pos: uint32(offset)})
	} else {
		gset, err := mysql.ParseGTIDSet(c.Flavor(), pos)
		if err != nil {
			return nil, nil, nil, mysql.Position{}, err
		}
		return c.startCdcStreamingGtid(gset)
	}
}

func (c *MySqlConnector) startCdcStreamingFilePos(
	pos mysql.Position,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	syncer := c.startSyncer()
	stream, err := syncer.StartSync(pos)
	if err != nil {
		syncer.Close()
	}
	return syncer, stream, nil, pos, err
}

func (c *MySqlConnector) startCdcStreamingGtid(
	gset mysql.GTIDSet,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	// https://hevodata.com/learn/mysql-gtids-and-replication-set-up
	syncer := c.startSyncer()
	stream, err := syncer.StartSyncGTID(gset)
	if err != nil {
		syncer.Close()
	}
	return syncer, stream, gset, mysql.Position{}, err
}

func (c *MySqlConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MySqlConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

func (c *MySqlConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

func (c *MySqlConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool *pgxpool.Pool,
	alertKeys *alerting.AlertKeys,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	return nil
}

func (c *MySqlConnector) GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error) {
	return nil, nil
}

func (c *MySqlConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	return nil
}

func (c *MySqlConnector) RemoveTablesFromPublication(ctx context.Context, req *protos.RemoveTablesFromPublicationInput) error {
	return nil
}

func (c *MySqlConnector) PullRecords(
	ctx context.Context,
	catalogPool *pgxpool.Pool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	syncer, mystream, gset, pos, err := c.startStreaming(req.LastOffset.Text)
	if err != nil {
		return err
	}
	defer syncer.Close()

	if gset == nil {
		req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
	}

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, req.IdleTimeout)
	defer cancelTimeout()

	var recordCount uint32
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", recordCount))
	}()

	for recordCount < req.MaxBatchSize {
		event, err := mystream.GetEvent(timeoutCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		if otelManager != nil {
			otelManager.Metrics.FetchedBytesCounter.Add(ctx, int64(len(event.RawData)))
		}

		if gset == nil && event.Header.LogPos > 0 {
			pos.Pos = max(pos.Pos, event.Header.LogPos)
			req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
		}

		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			if gset == nil && event.Header.Timestamp != 0 {
				pos.Name = string(ev.NextLogName)
				pos.Pos = uint32(ev.Position)
				req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
			}
		case *replication.MariadbGTIDEvent:
			if gset != nil {
				var err error
				newset, err := ev.GTIDNext()
				if err != nil {
					// TODO could ignore, but then we might get stuck rereading same batch each time
					return err
				}
				if err := gset.Update(newset.String()); err != nil {
					return err
				}
				req.RecordStream.UpdateLatestCheckpointText(gset.String())
			}
		case *replication.GTIDEvent:
			if gset != nil {
				var err error
				newset, err := ev.GTIDNext()
				if err != nil {
					// TODO could ignore, but then we might get stuck rereading same batch each time
					return err
				}
				if err := gset.Update(newset.String()); err != nil {
					return err
				}
				req.RecordStream.UpdateLatestCheckpointText(gset.String())
			}
		case *replication.PreviousGTIDsEvent:
			// TODO look into this, maybe we just do gset.Update(ev.GTIDSets)
		case *replication.RowsEvent:
			sourceTableName := string(ev.Table.Schema) + "." + string(ev.Table.Table) // TODO this is fragile
			destinationTableName := req.TableNameMapping[sourceTableName].Name
			schema := req.TableNameSchemaMapping[destinationTableName]
			if schema != nil {
				switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0:
					return errors.New("mysql v0 replication protocol not supported")
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
					for _, row := range ev.Rows {
						items := model.NewRecordItems(len(row))
						for idx, val := range row {
							fd := schema.Columns[idx]
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
						}

						recordCount += 1
						if err := req.RecordStream.AddRecord(ctx, &model.InsertRecord[model.RecordItems]{
							BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							Items:                items,
							SourceTableName:      sourceTableName,
							DestinationTableName: destinationTableName,
						}); err != nil {
							return err
						}
						if recordCount == 1 {
							req.RecordStream.SignalAsNotEmpty()
						}
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
					for idx := 0; idx < len(ev.Rows); idx += 2 {
						var unchangedToastColumns map[string]struct{}
						if len(ev.SkippedColumns) > idx+1 {
							unchangedToastColumns = make(map[string]struct{}, len(ev.SkippedColumns[idx+1]))
							for _, skipped := range ev.SkippedColumns[idx+1] {
								unchangedToastColumns[schema.Columns[skipped].Name] = struct{}{}
							}
						}

						oldRow := ev.Rows[idx]
						oldItems := model.NewRecordItems(len(oldRow))
						for idx, val := range oldRow {
							fd := schema.Columns[idx]
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							oldItems.AddColumn(fd.Name, val)
						}
						newRow := ev.Rows[idx+1]
						newItems := model.NewRecordItems(len(newRow))
						for idx, val := range ev.Rows[idx+1] {
							fd := schema.Columns[idx]
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							newItems.AddColumn(fd.Name, val)
						}

						recordCount += 1
						if err := req.RecordStream.AddRecord(ctx, &model.UpdateRecord[model.RecordItems]{
							BaseRecord:            model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							OldItems:              oldItems,
							NewItems:              newItems,
							SourceTableName:       sourceTableName,
							DestinationTableName:  destinationTableName,
							UnchangedToastColumns: unchangedToastColumns,
						}); err != nil {
							return err
						}
						if recordCount == 1 {
							req.RecordStream.SignalAsNotEmpty()
						}
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
					for idx, row := range ev.Rows {
						var unchangedToastColumns map[string]struct{}
						if len(ev.SkippedColumns) > idx {
							unchangedToastColumns = make(map[string]struct{}, len(ev.SkippedColumns[idx]))
							for _, skipped := range ev.SkippedColumns[idx] {
								unchangedToastColumns[schema.Columns[skipped].Name] = struct{}{}
							}
						}

						items := model.NewRecordItems(len(row))
						for idx, val := range row {
							fd := schema.Columns[idx]
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
						}

						recordCount += 1
						if err := req.RecordStream.AddRecord(ctx, &model.DeleteRecord[model.RecordItems]{
							BaseRecord:            model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							Items:                 items,
							SourceTableName:       sourceTableName,
							DestinationTableName:  destinationTableName,
							UnchangedToastColumns: unchangedToastColumns,
						}); err != nil {
							return err
						}
						if recordCount == 1 {
							req.RecordStream.SignalAsNotEmpty()
						}
					}
				default:
				}
			}
		}
	}
	return nil
}

func QValueFromMysqlRowEvent(mytype byte, qkind qvalue.QValueKind, val any) (qvalue.QValue, error) {
	// See go-mysql row_event.go for mapping
	switch val := val.(type) {
	case nil:
		return qvalue.QValueNull(qkind), nil
	case int8: // go-mysql reads all integers as signed, consumer needs to check metadata & convert
		if qkind == qvalue.QValueKindUInt8 {
			return qvalue.QValueUInt8{Val: uint8(val)}, nil
		} else {
			return qvalue.QValueInt8{Val: val}, nil
		}
	case int16:
		if qkind == qvalue.QValueKindUInt16 {
			return qvalue.QValueUInt16{Val: uint16(val)}, nil
		} else {
			return qvalue.QValueInt16{Val: val}, nil
		}
	case int32:
		if qkind == qvalue.QValueKindUInt32 {
			if mytype == mysql.MYSQL_TYPE_INT24 {
				return qvalue.QValueUInt32{Val: uint32(val) & 0xFFFFFF}, nil
			} else {
				return qvalue.QValueUInt32{Val: uint32(val)}, nil
			}
		} else {
			return qvalue.QValueInt32{Val: val}, nil
		}
	case int64:
		if qkind == qvalue.QValueKindUInt64 {
			return qvalue.QValueUInt64{Val: uint64(val)}, nil
		} else {
			return qvalue.QValueInt64{Val: val}, nil
		}
	case float32:
		return qvalue.QValueFloat32{Val: val}, nil
	case float64:
		return qvalue.QValueFloat64{Val: val}, nil
	case decimal.Decimal:
		return qvalue.QValueNumeric{Val: val}, nil
	case int:
		// YEAR: https://dev.mysql.com/doc/refman/8.4/en/year.html
		return qvalue.QValueInt16{Val: int16(val)}, nil
	case time.Time:
		return qvalue.QValueTimestamp{Val: val}, nil
	case *replication.JsonDiff:
		// TODO support somehow??
		return qvalue.QValueNull(qvalue.QValueKindJSON), nil
	case []byte:
		switch qkind {
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: val}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(val)}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: string(val)}, nil
		case qvalue.QValueKindGeometry:
			// TODO figure out mysql geo encoding
			return qvalue.QValueGeometry{Val: string(val)}, nil
		}
	case string:
		switch qkind {
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: []byte(val)}, nil
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: val}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: val}, nil
		case qvalue.QValueKindGeometry:
			// TODO figure out mysql geo encoding
			return qvalue.QValueGeometry{Val: val}, nil
		case qvalue.QValueKindTime:
			val, err := time.Parse("15:04:05.999999", val)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTime{Val: val}, nil
		case qvalue.QValueKindDate:
			val, err := time.Parse(time.DateOnly, val)
			if err != nil {
				return nil, err
			}
			return qvalue.QValueDate{Val: val}, nil
		}
	}
	return nil, fmt.Errorf("unexpected type %T for mysql type %d", val, mytype)
}
