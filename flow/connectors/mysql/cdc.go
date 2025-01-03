package connmysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/otel_metrics"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
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
		c.logger.Info("fetched schema for table " + tableName)
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
		qkind, err := qkindFromMysql(field.Type)
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

func (c *MySqlConnector) SetupReplConn(ctx context.Context) error {
	// mysql code will spin up new connection for each normalize for now
	flowName := ctx.Value(shared.FlowNameKey).(string)
	offset, err := c.GetLastOffset(ctx, flowName)
	if err != nil {
		return fmt.Errorf("[mysql] SetupReplConn failed to GetLastOffset: %w", err)
	}
	if offset.Text == "" {
		set, err := c.GetMasterGTIDSet(ctx)
		if err != nil {
			return fmt.Errorf("[mysql] SetupReplConn failed to GetMasterGTIDSet: %w", err)
		}
		if err := c.SetLastOffset(
			ctx, flowName, model.CdcCheckpoint{Text: set.String()},
		); err != nil {
			return fmt.Errorf("[mysql] SetupReplConn failed to SetLastOffset: %w", err)
		}
	}
	return nil
}

func (c *MySqlConnector) startSyncer() *replication.BinlogSyncer {
	//nolint:gosec
	return replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   rand.Uint32(),
		Flavor:     c.config.Flavor,
		Host:       c.config.Host,
		Port:       uint16(c.config.Port),
		User:       c.config.User,
		Password:   c.config.Password,
		UseDecimal: true,
		ParseTime:  true,
	})
}

//nolint:unused
func (c *MySqlConnector) startCdcStreamingFilePos(
	lastOffsetName string, lastOffsetPos uint32,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, error) {
	syncer := c.startSyncer()
	stream, err := syncer.StartSync(mysql.Position{Name: lastOffsetName, Pos: lastOffsetPos})
	return syncer, stream, err
}

func (c *MySqlConnector) startCdcStreamingGtid(gset mysql.GTIDSet) (*replication.BinlogSyncer, *replication.BinlogStreamer, error) {
	// https://hevodata.com/learn/mysql-gtids-and-replication-set-up
	syncer := c.startSyncer()
	stream, err := syncer.StartSyncGTID(gset)
	return syncer, stream, err
}

func (c *MySqlConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MySqlConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	// TODO assert c.replState == lastOffset
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
	gset, err := mysql.ParseGTIDSet(c.config.Flavor, req.LastOffset.Text)
	if err != nil {
		return err
	}

	syncer, mystream, err := c.startCdcStreamingGtid(gset)
	if err != nil {
		return err
	}
	defer syncer.Close()

	var fetchedBytesCounter metric.Int64Counter
	if otelManager != nil {
		var err error
		fetchedBytesCounter, err = otelManager.GetOrInitInt64Counter(otel_metrics.BuildMetricName(otel_metrics.FetchedBytesCounterName),
			metric.WithUnit("By"), metric.WithDescription("Bytes received of CopyData over replication slot"))
		if err != nil {
			return fmt.Errorf("could not get FetchedBytesCounter: %w", err)
		}
	}

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, req.IdleTimeout)
	defer cancelTimeout()

	var recordCount uint32
	for {
		event, err := mystream.GetEvent(timeoutCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		if fetchedBytesCounter != nil {
			fetchedBytesCounter.Add(ctx, int64(len(event.RawData)), metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowNameKey, req.FlowJobName),
			)))
		}

		switch ev := event.Event.(type) {
		case *replication.MariadbGTIDEvent:
			var err error
			newset, err := ev.GTIDNext()
			if err != nil {
				// TODO could ignore, but then we might get stuck rereading same batch each time
				return err
			}
			c.replState = newset
		case *replication.GTIDEvent:
			var err error
			newset, err := ev.GTIDNext()
			if err != nil {
				// TODO could ignore, but then we might get stuck rereading same batch each time
				return err
			}
			c.replState = newset
		case *replication.RowsEvent:
			sourceTableName := string(ev.Table.Schema) + "." + string(ev.Table.Table) // TODO this is fragile
			destinationTableName := req.TableNameMapping[sourceTableName].Name
			schema := req.TableNameSchemaMapping[destinationTableName]
			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0:
				return errors.New("mysql v0 replication protocol not supported")
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
				for _, row := range ev.Rows {
					items := model.NewRecordItems(len(row))
					for idx, val := range row {
						fd := schema.Columns[idx]
						items.AddColumn(fd.Name, qvalueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
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
				}
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
				// TODO populate UnchangedToastColumns with ev.SkippedColumns
				for idx := 0; idx < len(ev.Rows); idx += 2 {
					oldRow := ev.Rows[idx]
					oldItems := model.NewRecordItems(len(oldRow))
					for idx, val := range oldRow {
						fd := schema.Columns[idx]
						oldItems.AddColumn(fd.Name, qvalueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
					}
					newRow := ev.Rows[idx+1]
					newItems := model.NewRecordItems(len(newRow))
					for idx, val := range ev.Rows[idx+1] {
						fd := schema.Columns[idx]
						newItems.AddColumn(fd.Name, qvalueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
					}

					recordCount += 1
					if err := req.RecordStream.AddRecord(ctx, &model.UpdateRecord[model.RecordItems]{
						BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						OldItems:             oldItems,
						NewItems:             newItems,
						SourceTableName:      sourceTableName,
						DestinationTableName: destinationTableName,
					}); err != nil {
						return err
					}
				}
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
				for _, row := range ev.Rows {
					items := model.NewRecordItems(len(row))
					for idx, val := range row {
						fd := schema.Columns[idx]
						items.AddColumn(fd.Name, qvalueFromMysqlRowEvent(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
					}

					recordCount += 1
					if err := req.RecordStream.AddRecord(ctx, &model.DeleteRecord[model.RecordItems]{
						BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						Items:                items,
						SourceTableName:      sourceTableName,
						DestinationTableName: destinationTableName,
					}); err != nil {
						return err
					}
				}
			default:
				continue
			}
		}

		if recordCount >= req.MaxBatchSize {
			return nil
		}
	}
}

func qvalueFromMysqlRowEvent(mytype byte, qkind qvalue.QValueKind, val any) qvalue.QValue {
	// TODO signedness, in ev.Table, need to extend QValue system
	// See go-mysql row_event.go for mapping
	switch val := val.(type) {
	case nil:
		return qvalue.QValueNull(qkind)
	case int8: // TODO qvalue.Int8
		return qvalue.QValueInt16{Val: int16(val)}
	case int16:
		return qvalue.QValueInt16{Val: val}
	case int32:
		return qvalue.QValueInt32{Val: val}
	case int64:
		return qvalue.QValueInt64{Val: val}
	case float32:
		return qvalue.QValueFloat32{Val: val}
	case float64:
		return qvalue.QValueFloat64{Val: val}
	case decimal.Decimal:
		return qvalue.QValueNumeric{Val: val}
	case int:
		// YEAR: https://dev.mysql.com/doc/refman/8.4/en/year.html
		return qvalue.QValueInt16{Val: int16(val)}
	case time.Time:
		return qvalue.QValueTimestamp{Val: val}
	case *replication.JsonDiff:
		// TODO support somehow??
		return qvalue.QValueNull(qvalue.QValueKindJSON)
	case []byte:
		switch mytype {
		case mysql.MYSQL_TYPE_BLOB:
			return qvalue.QValueBytes{Val: val}
		case mysql.MYSQL_TYPE_JSON:
			return qvalue.QValueJSON{Val: string(val)}
		case mysql.MYSQL_TYPE_GEOMETRY:
			// TODO figure out mysql geo encoding
			return qvalue.QValueGeometry{Val: string(val)}
		}
	case string:
		switch mytype {
		case mysql.MYSQL_TYPE_TIME:
			// TODO parse
		case mysql.MYSQL_TYPE_TIME2:
			// TODO parse
		case mysql.MYSQL_TYPE_DATE:
			// TODO parse
		case mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_VAR_STRING,
			mysql.MYSQL_TYPE_STRING:
			return qvalue.QValueString{Val: val}
		}
	}
	panic(fmt.Sprintf("unexpected type %T for mysql type %d", val, mytype))
}
