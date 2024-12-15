package connmysql

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jackc/pgx/v5/pgxpool"

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

func (c *MySqlConnector) EnsurePullability(ctx context.Context, req *protos.EnsurePullabilityBatchInput) (
	*protos.EnsurePullabilityBatchOutput, error)

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

func qvalueFromMysql(typ byte, val any) qvalue.QValue {
	// TODO
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
	for {
		event, err := mystream.GetEvent(ctx)
		if err != nil {
			return err
		}
		switch ev := event.Event.(type) {
		case *replication.RowsEvent:
			for _, row := range ev.Rows {
				var record model.Record[model.RecordItems]
				//TODO need tableNameMapping[source] -> destination
				//TODO need mapping of column index to column name
				var items model.RecordItems
				switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					for idx, val := range row {
						// TODO
						items.AddColumn("ColumnName", qvalueFromMysql(ev.Table.ColumnType[idx], val))
					}
					record = &model.InsertRecord[model.RecordItems]{
						BaseRecord:      model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						Items:           items,
						SourceTableName: string(ev.Table.Table),
					}
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					// TODO no OldItems / NewItems. How does primary key update work?
					record = &model.UpdateRecord[model.RecordItems]{
						BaseRecord:      model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						NewItems:        items,
						SourceTableName: string(ev.Table.Table),
					}
				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					record = &model.DeleteRecord[model.RecordItems]{
						BaseRecord:      model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
						Items:           items,
						SourceTableName: string(ev.Table.Table),
					}
				default:
					continue
				}
				err := req.RecordStream.AddRecord(ctx, record)
				if err != nil {
					return err
				}
			}
			break
		}
		break // TODO when batch ready
	}
	return nil
}
