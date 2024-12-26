package connmysql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/otel_metrics"
)

func (c *MySqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	system protos.TypeSystem,
	tableIdentifiers []string,
) (map[string]*protos.TableSchema, error) {
	panic("TODO")
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

func (c *MySqlConnector) SetupReplConn(context.Context) error {
	// mysql code will spin up new connection for each normalize for now
	return nil
}

//nolint:unused
func (c *MySqlConnector) startCdcStreamingFilePos(lastOffsetName string, lastOffsetPos uint32) (*replication.BinlogStreamer, error) {
	return c.syncer.StartSync(mysql.Position{Name: lastOffsetName, Pos: lastOffsetPos})
}

func (c *MySqlConnector) startCdcStreamingGtid(gset mysql.GTIDSet) (*replication.BinlogStreamer, error) {
	// https://hevodata.com/learn/mysql-gtids-and-replication-set-up
	return c.syncer.StartSyncGTID(gset)
}

func (c *MySqlConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MySqlConnector) UpdateReplStateLastOffset(lastOffset model.CdcCheckpoint) {
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

func qvalueFromMysql(mytype byte, qkind qvalue.QValueKind, val any) qvalue.QValue {
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
	default:
		panic(fmt.Sprintf("unexpected type %T for mysql type %d", val, mytype))
	}
	return nil
}

func (c *MySqlConnector) PullRecords(
	ctx context.Context,
	catalogPool *pgxpool.Pool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer func() {
		req.RecordStream.Close()
		// update replState Offset
	}()
	gset, err := mysql.ParseGTIDSet(c.config.Flavor, req.LastOffset.Text)
	if err != nil {
		return err
	}
	mystream, err := c.startCdcStreamingGtid(gset)
	if err != nil {
		return err
	}

	var fetchedBytesCounter metric.Int64Counter
	if otelManager != nil {
		var err error
		fetchedBytesCounter, err = otelManager.GetOrInitInt64Counter(otel_metrics.BuildMetricName(otel_metrics.FetchedBytesCounterName),
			metric.WithUnit("By"), metric.WithDescription("Bytes received of CopyData over replication slot"))
		if err != nil {
			return fmt.Errorf("could not get FetchedBytesCounter: %w", err)
		}
	}

	var recordCount uint32
	for {
		// TODO put req.IdleTimeout timer on this
		event, err := mystream.GetEvent(ctx)
		if err != nil {
			return err
		}

		if fetchedBytesCounter != nil {
			fetchedBytesCounter.Add(ctx, int64(len(event.RawData)), metric.WithAttributeSet(attribute.NewSet(
				attribute.String(otel_metrics.FlowNameKey, req.FlowJobName),
			)))
		}

		if ev, ok := event.Event.(*replication.RowsEvent); ok {
			sourceTableName := string(ev.Table.Table) // TODO need ev.Table.Schema?
			destinationTableName := req.TableNameMapping[sourceTableName].Name
			schema := req.TableNameSchemaMapping[destinationTableName]
			for _, row := range ev.Rows {
				var record model.Record[model.RecordItems]
				// TODO need mapping of column index to column name
				var items model.RecordItems
				switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0:
					return errors.New("mysql v0 replication protocol not supported")
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					for idx, val := range row {
						fd := schema.Columns[idx]
						items.AddColumn(fd.Name, qvalueFromMysql(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
					}
					record = &model.InsertRecord[model.RecordItems]{
						BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						Items:                items,
						SourceTableName:      sourceTableName,
						DestinationTableName: destinationTableName,
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					var oldItems model.RecordItems
					for idx, val := range row {
						fd := schema.Columns[idx>>1]
						qv := qvalueFromMysql(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val)
						if (idx & 1) == 0 { // TODO test that it isn't other way around
							oldItems.AddColumn(fd.Name, qv)
						} else {
							items.AddColumn(fd.Name, qv)
						}
					}
					record = &model.UpdateRecord[model.RecordItems]{
						BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						OldItems:             oldItems,
						NewItems:             items,
						SourceTableName:      sourceTableName,
						DestinationTableName: destinationTableName,
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					for idx, val := range row {
						fd := schema.Columns[idx]
						items.AddColumn(fd.Name, qvalueFromMysql(ev.Table.ColumnType[idx], qvalue.QValueKind(fd.Type), val))
					}
					record = &model.DeleteRecord[model.RecordItems]{
						BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						Items:                items,
						SourceTableName:      sourceTableName,
						DestinationTableName: destinationTableName,
					}
				default:
					continue
				}
				recordCount += 1
				if err := req.RecordStream.AddRecord(ctx, record); err != nil {
					return err
				}
			}
		}

		if recordCount >= req.MaxBatchSize {
			return nil
		}
	}
}
