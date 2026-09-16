package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/civil"
	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const queryCDCReadStreams = 4

func cloneInflightState(state *model.QueryCDCInflightState) *model.QueryCDCInflightState {
	clone := *state
	clone.ArrowSchema = slices.Clone(state.ArrowSchema)
	clone.Streams = slices.Clone(state.Streams)
	return &clone
}

func (c *BigQueryConnector) createQueryCDCReadSession(
	ctx context.Context, req *model.PullTableRecordsRequest, tm *protos.TableMapping,
	queryMode bool, start, end time.Time, sourceHasMore bool,
) (*model.QueryCDCInflightState, bigquery.Schema, error) {
	dsTable, err := c.convertToDatasetTable(req.SourceTableIdentifier)
	if err != nil {
		return nil, nil, err
	}
	if queryMode && tm.QueryCdcWatermarkColumn == "" {
		return nil, nil, fmt.Errorf("query CDC watermark column is empty for %s", req.SourceTableIdentifier)
	}
	buildQuery := func(exclude map[string]struct{}) string {
		if queryMode {
			return buildWatermarkPullQuery(dsTable.stringQuoted(), tm.QueryCdcWatermarkColumn, exclude)
		}
		fn := "APPENDS"
		orderBy := ""
		if tm.BigqueryCdcEventsFunction == protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_CHANGES {
			fn = "CHANGES"
			orderBy = quotedIdentifier(bigQueryChangeTimestampColumn)
		} else if tm.BigqueryCdcEventsFunction != protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS {
			return ""
		}
		return buildPullQuery(fn, dsTable.stringQuoted(), exclude, orderBy)
	}

	effective := c.effectiveExclude(req.SourceTableIdentifier, req.NameAndExclude.Exclude)
	if query := buildQuery(effective); query == "" {
		return nil, nil, fmt.Errorf("unsupported BigQuery CDC events function: %v", tm.BigqueryCdcEventsFunction)
	}
	var destination *bigquery.Table
	for {
		q := c.client.Query(buildQuery(effective))
		q.Parameters = []bigquery.QueryParameter{{Name: "start", Value: start}, {Name: "end", Value: end}}
		job, runErr := q.Run(ctx)
		if runErr != nil {
			return nil, nil, runErr
		}
		status, waitErr := job.Wait(ctx)
		if waitErr != nil {
			return nil, nil, waitErr
		}
		if statusErr := status.Err(); statusErr != nil {
			missing := missingExceptColumns(statusErr, effective)
			if len(missing) == 0 {
				return nil, nil, statusErr
			}
			effective = c.dropMissingExcludeColumns(req.SourceTableIdentifier, effective, missing)
			continue
		}
		jobConfig, configErr := job.Config()
		if configErr != nil {
			return nil, nil, configErr
		}
		queryConfig, ok := jobConfig.(*bigquery.QueryConfig)
		if !ok || queryConfig.Dst == nil {
			return nil, nil, fmt.Errorf("query job returned no result table")
		}
		destination = queryConfig.Dst
		break
	}

	metadata, err := destination.Metadata(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get query result schema: %w", err)
	}
	tableResource := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", destination.ProjectID, destination.DatasetID, destination.TableID)
	session, err := c.storageReadClient.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent:         fmt.Sprintf("projects/%s", destination.ProjectID),
		ReadSession:    &storagepb.ReadSession{Table: tableResource, DataFormat: storagepb.DataFormat_ARROW},
		MaxStreamCount: queryCDCReadStreams,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create Storage Read session: %w", err)
	}
	if session.GetArrowSchema() == nil {
		return nil, nil, fmt.Errorf("Storage Read session returned no Arrow schema")
	}
	state := &model.QueryCDCInflightState{
		Version: 1, WindowStart: start, WindowEnd: end, ResultTable: tableResource,
		SessionName: session.Name, SessionExpiresAt: session.ExpireTime.AsTime(),
		ArrowSchema: slices.Clone(session.GetArrowSchema().GetSerializedSchema()), SourceHasMore: sourceHasMore,
		Streams: make([]model.QueryCDCStreamState, len(session.Streams)),
	}
	for i, stream := range session.Streams {
		state.Streams[i].Name = stream.Name
	}
	return state, metadata.Schema, nil
}

func (c *BigQueryConnector) dropMissingExcludeColumns(
	table string, effective, missing map[string]struct{},
) map[string]struct{} {
	c.droppedExcludeColumnsMu.Lock()
	defer c.droppedExcludeColumnsMu.Unlock()
	dropped := c.droppedExcludeColumns[table]
	if dropped == nil {
		dropped = make(map[string]struct{})
		c.droppedExcludeColumns[table] = dropped
	}
	next := make(map[string]struct{}, len(effective))
	for col := range effective {
		if _, gone := missing[col]; gone {
			dropped[col] = struct{}{}
			c.logger.Warn("[bigquery] excluded column no longer exists on source table, dropping from EXCEPT clause",
				slog.String("table", table), slog.String("column", col))
			continue
		}
		next[col] = struct{}{}
	}
	return next
}

func parseBigQueryTableResource(resource string) (project, dataset, table string, err error) {
	parts := strings.Split(resource, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" {
		return "", "", "", fmt.Errorf("invalid BigQuery table resource %q", resource)
	}
	return parts[1], parts[3], parts[5], nil
}

func (c *BigQueryConnector) queryCDCResultSchema(ctx context.Context, resource string) (bigquery.Schema, error) {
	project, dataset, table, err := parseBigQueryTableResource(resource)
	if err != nil {
		return nil, err
	}
	metadata, err := c.client.DatasetInProject(project, dataset).Table(table).Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return metadata.Schema, nil
}

func queryCDCSliceLimitReached(rows, bytes int64, elapsed time.Duration, maxRows, maxBytes int64, maxDuration time.Duration) bool {
	return (maxRows > 0 && rows >= maxRows) || (maxBytes > 0 && bytes >= maxBytes) ||
		(maxDuration > 0 && elapsed >= maxDuration)
}

func (c *BigQueryConnector) pullQueryCDCStorageSlice(
	ctx context.Context, req *model.PullTableRecordsRequest, tm *protos.TableMapping,
	queryMode bool, start, end time.Time, sourceHasMore bool,
	addRecord func(context.Context, model.Record[model.RecordItems]) error,
) (*model.QueryCDCInflightState, bool, int64, error) {
	state := req.InflightState
	var schema bigquery.Schema
	var err error
	if state == nil {
		state, schema, err = c.createQueryCDCReadSession(ctx, req, tm, queryMode, start, end, sourceHasMore)
	} else {
		state = cloneInflightState(state)
		if state.Version != 1 {
			return nil, false, 0, fmt.Errorf("unsupported query CDC in-flight state version %d", state.Version)
		}
		if !state.SessionExpiresAt.IsZero() && time.Now().After(state.SessionExpiresAt) {
			return nil, false, 0, fmt.Errorf("Storage Read session %s expired at %s", state.SessionName, state.SessionExpiresAt)
		}
		schema, err = c.queryCDCResultSchema(ctx, state.ResultTable)
	}
	if err != nil {
		return nil, false, 0, err
	}
	decoder, err := newBQArrowDecoder(state.ArrowSchema, schema)
	if err != nil {
		return nil, false, 0, fmt.Errorf("decode Arrow schema: %w", err)
	}

	maxRows, err := internal.PeerDBBigQueryCDCPullSliceMaxRows(ctx, req.Env)
	if err != nil {
		return nil, false, 0, err
	}
	maxBytes, err := internal.PeerDBBigQueryCDCPullSliceMaxBytes(ctx, req.Env)
	if err != nil {
		return nil, false, 0, err
	}
	maxDuration, err := internal.PeerDBBigQueryCDCPullSliceMaxDuration(ctx, req.Env)
	if err != nil {
		return nil, false, 0, err
	}

	started := time.Now()
	var rowsRead, bytesRead atomic.Int64
	var stop atomic.Bool
	readCtx, cancelReads := context.WithCancel(ctx)
	defer cancelReads()
	var stopOnce sync.Once
	stopReading := func() {
		stopOnce.Do(func() {
			stop.Store(true)
			cancelReads()
		})
	}
	var durationTimer *time.Timer
	if maxDuration > 0 {
		durationTimer = time.AfterFunc(maxDuration, stopReading)
		defer durationTimer.Stop()
	}
	qfields := make([]types.QField, len(schema))
	for i, field := range schema {
		qfields[i] = BigQueryFieldToQField(field)
	}
	watermarkIdx := -1
	changeCols := locateBigQueryChangeColumns(schema)
	if queryMode {
		watermarkIdx = slices.IndexFunc(schema, func(field *bigquery.FieldSchema) bool {
			return field.Name == tm.QueryCdcWatermarkColumn
		})
	} else if changeCols.changeType < 0 {
		return nil, false, 0, fmt.Errorf("Storage Read result for %s has no %s column", req.SourceTableIdentifier, bigQueryChangeTypeColumn)
	}
	var group errgroup.Group
	for streamIdx := range state.Streams {
		stream := &state.Streams[streamIdx]
		if stream.Complete {
			continue
		}
		group.Go(func() error {
			if stop.Load() {
				return nil
			}
			reader, err := c.storageReadClient.ReadRows(readCtx, &storagepb.ReadRowsRequest{
				ReadStream: stream.Name, Offset: stream.CommittedOffset,
			})
			if err != nil {
				if stop.Load() && readCtx.Err() != nil {
					return nil
				}
				cancelReads()
				return err
			}
			for {
				if stop.Load() {
					return nil
				}
				response, recvErr := reader.Recv()
				if errors.Is(recvErr, io.EOF) {
					stream.Complete = true
					return nil
				}
				if recvErr != nil {
					if stop.Load() && readCtx.Err() != nil {
						return nil
					}
					cancelReads()
					return fmt.Errorf("read %s at offset %d: %w", stream.Name, stream.CommittedOffset, recvErr)
				}
				batch := response.GetArrowRecordBatch()
				responseRows := response.GetRowCount()
				if responseRows == 0 {
					continue
				}
				if batch == nil {
					cancelReads()
					return fmt.Errorf("Storage Read response has %d rows but no Arrow batch", responseRows)
				}
				rows, err := decoder.decode(batch.SerializedRecordBatch)
				if err != nil {
					cancelReads()
					return err
				}
				if int64(len(rows)) != responseRows {
					cancelReads()
					return fmt.Errorf(
						"Storage Read response row count mismatch: response=%d decoded=%d", responseRows, len(rows))
				}
				for _, row := range rows {
					record, skip, err := queryCDCRecord(queryMode, tm, req.SourceTableIdentifier, req.NameAndExclude.Name,
						state.WindowStart, schema, qfields, watermarkIdx, changeCols, row)
					if err != nil {
						cancelReads()
						return err
					}
					if !skip {
						if err := addRecord(ctx, record); err != nil {
							cancelReads()
							return err
						}
					}
				}
				// Commit only complete ReadRowsResponse boundaries. The activity does
				// not persist this candidate until destination staging succeeds.
				stream.CommittedOffset += responseRows
				currentRows := rowsRead.Add(responseRows)
				currentBytes := bytesRead.Add(int64(len(batch.SerializedRecordBatch)))
				if queryCDCSliceLimitReached(currentRows, currentBytes, time.Since(started), maxRows, maxBytes, maxDuration) {
					stopReading()
					return nil
				}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, false, bytesRead.Load(), err
	}
	complete := true
	for i := range state.Streams {
		if !state.Streams[i].Complete {
			complete = false
			break
		}
	}
	return state, complete, bytesRead.Load(), nil
}

func queryCDCRecord(
	queryMode bool, tm *protos.TableMapping, sourceTable, destinationTable string, windowStart time.Time,
	schema bigquery.Schema, qfields []types.QField, watermarkIdx int, cols bigQueryChangeColumns, row []bigquery.Value,
) (model.Record[model.RecordItems], bool, error) {
	commitTimeNano := windowStart.UnixNano()
	if queryMode {
		if watermarkIdx >= 0 {
			switch value := row[watermarkIdx].(type) {
			case time.Time:
				commitTimeNano = value.UnixNano()
			case civil.Date:
				commitTimeNano = value.In(time.UTC).UnixNano()
			}
		}
	} else if cols.changeTimestamp >= 0 {
		if value, ok := row[cols.changeTimestamp].(time.Time); ok {
			commitTimeNano = value.UnixNano()
		}
	}
	changeType := bigQueryChangeTypeInsert
	isForUpdate := false
	if !queryMode {
		changeType, _ = row[cols.changeType].(string)
		if cols.isForUpdate >= 0 {
			isForUpdate, _ = row[cols.isForUpdate].(bool)
		}
	}
	if changeType == bigQueryChangeTypeDelete && isForUpdate {
		return nil, true, nil
	}
	items, err := bigQueryRowToRecordItems(schema, qfields, row)
	if err != nil {
		return nil, false, err
	}
	base := model.BaseRecord{CommitTimeNano: commitTimeNano}
	switch changeType {
	case bigQueryChangeTypeInsert:
		return &model.InsertRecord[model.RecordItems]{BaseRecord: base, Items: items, SourceTableName: sourceTable, DestinationTableName: destinationTable}, false, nil
	case bigQueryChangeTypeUpdate:
		return &model.UpdateRecord[model.RecordItems]{BaseRecord: base, NewItems: items, SourceTableName: sourceTable, DestinationTableName: destinationTable}, false, nil
	case bigQueryChangeTypeDelete:
		return &model.DeleteRecord[model.RecordItems]{BaseRecord: base, Items: items, SourceTableName: sourceTable, DestinationTableName: destinationTable}, false, nil
	default:
		return nil, false, fmt.Errorf("unexpected _CHANGE_TYPE %q for table %s", changeType, sourceTable)
	}
}
