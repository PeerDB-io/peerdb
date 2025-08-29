package connmssql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MsSqlConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	if err := c.checkCdcEnabled(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (c *MsSqlConnector) ExportTxSnapshot(context.Context, string, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *MsSqlConnector) FinishExport(any) error {
	return nil
}

func (c *MsSqlConnector) SetupReplication(
	ctx context.Context,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	if err := c.checkCdcEnabled(ctx); err != nil {
		return model.SetupReplicationResult{}, err
	}

	var lsn []byte
	err := c.conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&lsn)
	if err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mssql] SetupReplication failed to get max LSN: %w", err)
	}

	lsnHex := hex.EncodeToString(lsn)
	if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: lsnHex}); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mssql] SetupReplication failed to SetLastOffset: %w", err)
	}

	c.logger.Info("[mssql] SetupReplication complete", slog.String("lsn", lsnHex))
	return model.SetupReplicationResult{}, nil
}

func (c *MsSqlConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

func (c *MsSqlConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MsSqlConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

func (c *MsSqlConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

type tableInfo struct {
	captureInstance string
	columns         []string
}

func (c *MsSqlConnector) captureInstanceForTable(ctx context.Context, schema, table string) (string, error) {
	var captureInstance string
	err := c.conn.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT TOP 1 capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(%s) ORDER BY create_date DESC",
		sanitize.QuoteString(schema+"."+table))).Scan(&captureInstance)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("[mssql] no CDC capture instance for %s.%s, enable CDC on this table", schema, table)
		}
		return "", fmt.Errorf("[mssql] failed to look up capture instance for %s.%s: %w", schema, table, err)
	}
	return captureInstance, nil
}

func (c *MsSqlConnector) getCapturedColumns(ctx context.Context, captureInstance string) ([]string, error) {
	rows, err := c.conn.QueryContext(ctx, fmt.Sprintf(
		"SELECT column_name FROM cdc.captured_columns WHERE capture_instance = %s ORDER BY column_ordinal",
		sanitize.QuoteString(captureInstance)))
	if err != nil {
		return nil, fmt.Errorf("[mssql] failed to get captured columns for %s: %w", captureInstance, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func (c *MsSqlConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return err
	}

	var lastLsn []byte
	if req.LastOffset.Text != "" {
		lastLsn, err = hex.DecodeString(req.LastOffset.Text)
		if err != nil {
			return fmt.Errorf("[mssql] failed to parse last LSN offset %q: %w", req.LastOffset.Text, err)
		}
	}

	tableInfos := make(map[string]*tableInfo, len(req.TableNameMapping))
	for sourceTable := range req.TableNameMapping {
		parts := strings.SplitN(sourceTable, ".", 2)
		if len(parts) != 2 {
			return fmt.Errorf("[mssql] invalid table name %q", sourceTable)
		}
		ci, err := c.captureInstanceForTable(ctx, parts[0], parts[1])
		if err != nil {
			return err
		}
		cols, err := c.getCapturedColumns(ctx, ci)
		if err != nil {
			return err
		}
		tableInfos[sourceTable] = &tableInfo{captureInstance: ci, columns: cols}
	}

	var recordCount uint32
	var fetchedBytes, totalFetchedBytes atomic.Int64
	pullStart := time.Now()

	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info("[mssql] PullRecords finished",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()

	defer func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
	}()

	// periodic metrics and logging
	shutdown := common.Interval(ctx, time.Minute, func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		c.logger.Info("[mssql] pulling records",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	})
	defer shutdown()

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
		}
		return nil
	}

	pollDeadline := time.Now().Add(time.Hour)

	for recordCount < req.MaxBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}

		var fromLsn, toLsn []byte
		if isZeroLsn(lastLsn) {
			err := c.conn.QueryRowContext(ctx,
				"SELECT sys.fn_cdc_get_max_lsn()").Scan(&toLsn)
			if err != nil {
				return fmt.Errorf("[mssql] failed to get max LSN: %w", err)
			}
		} else {
			err := c.conn.QueryRowContext(ctx,
				"SELECT sys.fn_cdc_increment_lsn(@p1), sys.fn_cdc_get_max_lsn()", lastLsn).Scan(&fromLsn, &toLsn)
			if err != nil {
				return fmt.Errorf("[mssql] failed to get LSN range: %w", err)
			}
		}

		if isZeroLsn(toLsn) || (!isZeroLsn(fromLsn) && bytes.Compare(fromLsn, toLsn) > 0) {
			if time.Now().After(pollDeadline) {
				c.logger.Info("[mssql] idle, no new changes")
				return nil
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		toLsnHex := hex.EncodeToString(toLsn)

		for sourceTable, nameAndExclude := range req.TableNameMapping {
			ti := tableInfos[sourceTable]
			schema := req.TableNameSchemaMapping[nameAndExclude.Name]
			if schema == nil {
				continue
			}

			tableFromLsn := fromLsn
			var minLsn []byte
			err := c.conn.QueryRowContext(ctx,
				"SELECT sys.fn_cdc_get_min_lsn(@p1)", ti.captureInstance).Scan(&minLsn)
			if err != nil {
				return fmt.Errorf("[mssql] failed to get min LSN for %s: %w", ti.captureInstance, err)
			}
			if isZeroLsn(tableFromLsn) {
				tableFromLsn = minLsn
			} else if bytes.Compare(tableFromLsn, minLsn) < 0 {
				return fmt.Errorf("[mssql] stored offset is behind CDC cleanup for %s, data may have been lost", sourceTable)
			}

			if isZeroLsn(tableFromLsn) {
				continue
			}

			if err := c.RetryOnDeadlock(func() error {
				return c.processTableChanges(ctx, otelManager, addRecord,
					ti, tableFromLsn, toLsn, schema, sourceTable, nameAndExclude,
					sourceSchemaAsDestinationColumn, &fetchedBytes, &totalFetchedBytes)
			}); err != nil {
				return err
			}
		}

		req.RecordStream.UpdateLatestCheckpointText(toLsnHex)
		lastLsn = toLsn

		if recordCount == 0 {
			pollDeadline = time.Now().Add(req.IdleTimeout)
		} else {
			break
		}
	}

	return nil
}

func (c *MsSqlConnector) processTableChanges(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
	ti *tableInfo,
	tableFromLsn, toLsn []byte,
	schema *protos.TableSchema,
	sourceTable string,
	nameAndExclude model.NameAndExclude,
	sourceSchemaAsDestinationColumn bool,
	fetchedBytes, totalFetchedBytes *atomic.Int64,
) error {
	query := "SELECT * FROM cdc.fn_cdc_get_all_changes_" + //nolint:gosec // capture instance from cdc.change_tables
		ti.captureInstance + "(@p1, @p2, N'all update old') ORDER BY __$start_lsn, __$seqval"

	rows, err := c.conn.QueryContext(ctx, query, tableFromLsn, toLsn)
	if err != nil {
		return fmt.Errorf("[mssql] failed to query changes for %s: %w", sourceTable, err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("[mssql] failed to get column types: %w", err)
	}

	// fn_cdc_get_all_changes returns: __$start_lsn, __$seqval, __$operation, __$update_mask, <captured_cols...>
	cdcMetaCols := 4
	capturedCols := ti.columns
	colQKinds := make([]types.QValueKind, len(capturedCols))
	for i, colName := range capturedCols {
		for _, fd := range schema.Columns {
			if fd.Name == colName {
				colQKinds[i] = types.QValueKind(fd.Type)
				break
			}
		}
	}

	var pendingOldItems model.RecordItems
	lsnCommitTimeCache := make(map[string]int64)

	for rows.Next() {
		scanArgs := make([]any, len(colTypes))
		for i := range scanArgs {
			scanArgs[i] = new(any)
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("[mssql] scan error: %w", err)
		}

		startLsn := *(scanArgs[0].(*any))
		operation := *(scanArgs[2].(*any))
		opInt, ok := operation.(int64)
		if !ok {
			return fmt.Errorf("[mssql] unexpected __$operation type %T", operation)
		}

		var commitTimeNano int64
		if lsnBytes, ok := startLsn.([]byte); ok {
			lsnKey := hex.EncodeToString(lsnBytes)
			if cached, found := lsnCommitTimeCache[lsnKey]; found {
				commitTimeNano = cached
			} else {
				var commitTime time.Time
				if err := c.conn.QueryRowContext(ctx,
					"SELECT sys.fn_cdc_map_lsn_to_time(@p1)", lsnBytes).Scan(&commitTime); err == nil {
					commitTimeNano = commitTime.UnixNano()
					otelManager.Metrics.CommitLagGauge.Record(ctx,
						time.Now().UTC().Sub(commitTime.UTC()).Microseconds())
				}
				lsnCommitTimeCache[lsnKey] = commitTimeNano
			}
		}

		items := model.NewRecordItems(len(capturedCols))
		for i, colName := range capturedCols {
			if nameAndExclude.Exclude != nil {
				if _, excluded := nameAndExclude.Exclude[colName]; excluded {
					continue
				}
			}
			rawVal := *(scanArgs[cdcMetaCols+i].(*any))
			qv, err := QValueFromMssqlValue(colQKinds[i], rawVal)
			if err != nil {
				return fmt.Errorf("[mssql] convert error for column %s: %w", colName, err)
			}
			items.AddColumn(colName, qv)
		}
		if sourceSchemaAsDestinationColumn {
			parts := strings.SplitN(sourceTable, ".", 2)
			if len(parts) == 2 {
				items.AddColumn("_peerdb_source_schema", types.QValueString{Val: parts[0]})
			}
		}

		switch opInt {
		case 2: // insert
			if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: commitTimeNano},
				Items:                items,
				SourceTableName:      sourceTable,
				DestinationTableName: nameAndExclude.Name,
			}); err != nil {
				return err
			}
		case 1: // delete
			if err := addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: commitTimeNano},
				Items:                items,
				SourceTableName:      sourceTable,
				DestinationTableName: nameAndExclude.Name,
			}); err != nil {
				return err
			}
		case 3: // update before image
			pendingOldItems = items
		case 4: // update after image
			if err := addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
				BaseRecord:           model.BaseRecord{CommitTimeNano: commitTimeNano},
				OldItems:             pendingOldItems,
				NewItems:             items,
				SourceTableName:      sourceTable,
				DestinationTableName: nameAndExclude.Name,
			}); err != nil {
				return err
			}
			pendingOldItems = model.RecordItems{}
		}

		rowBytes := c.deltaBytesRead.Swap(0)
		fetchedBytes.Add(rowBytes)
		totalFetchedBytes.Add(rowBytes)

		if commitTimeNano > 0 {
			otelManager.Metrics.LatestConsumedLogEventGauge.Record(ctx, commitTimeNano/1e9)
		}
	}
	return rows.Err()
}

func isZeroLsn(lsn []byte) bool {
	return !slices.ContainsFunc(lsn, func(b byte) bool { return b != 0 })
}
