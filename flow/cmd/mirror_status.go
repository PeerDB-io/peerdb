package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

func (h *FlowRequestHandler) ListMirrors(
	ctx context.Context,
	req *protos.ListMirrorsRequest,
) (*protos.ListMirrorsResponse, error) {
	rows, err := h.pool.Query(ctx, `select distinct on(f.name)
	  f.id, f.workflow_id, f.name,
	  sp.name source_name, sp.type source_type,
	  dp.name destination_name, dp.type source_type,
	  f.created_at, coalesce(f.query_string, '')='' is_cdc
	from flows f
	join peers sp on sp.id = f.source_peer
	join peers dp on dp.id = f.destination_peer`)
	if err != nil {
		return nil, err
	}
	mirrors, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.ListMirrorsItem, error) {
		var item protos.ListMirrorsItem
		var createdAt time.Time
		if err := row.Scan(
			&item.Id, &item.WorkflowId, &item.Name,
			&item.SourceName, &item.SourceType,
			&item.DestinationName, &item.DestinationType,
			&createdAt, &item.IsCdc,
		); err != nil {
			return nil, err
		}
		item.CreatedAt = float64(createdAt.UnixMilli())
		return &item, nil
	})
	if err != nil {
		return nil, err
	}
	return &protos.ListMirrorsResponse{
		Mirrors: mirrors,
	}, nil
}

func (h *FlowRequestHandler) MirrorStatus(
	ctx context.Context,
	req *protos.MirrorStatusRequest,
) (*protos.MirrorStatusResponse, error) {
	slog.Info("Mirror status endpoint called",
		slog.Bool("includeFlowInfo", req.IncludeFlowInfo),
		slog.String(string(shared.FlowNameKey), req.FlowJobName))

	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("unable to get the workflow ID of mirror", slog.Any("error", err))
		return &protos.MirrorStatusResponse{
			FlowJobName:      req.FlowJobName,
			CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
			ErrorMessage:     "unable to get the workflow ID of mirror " + req.FlowJobName,
			Ok:               false,
		}, nil
	}

	currState, err := h.getWorkflowStatus(ctx, workflowID)
	if err != nil {
		slog.Error("unable to get the running status of mirror", slog.Any("error", err))
		return &protos.MirrorStatusResponse{
			FlowJobName:      req.FlowJobName,
			CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
			ErrorMessage:     "unable to get the running status of mirror " + req.FlowJobName,
			Ok:               false,
		}, nil
	}

	createdAt, err := h.getMirrorCreatedAt(ctx, req.FlowJobName)
	if err != nil {
		return &protos.MirrorStatusResponse{
			FlowJobName:      req.FlowJobName,
			CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
			ErrorMessage:     "unable to get the creation time of mirror " + req.FlowJobName,
			Ok:               false,
		}, nil
	}

	if req.IncludeFlowInfo {
		cdcFlow, err := h.isCDCFlow(ctx, req.FlowJobName)
		if err != nil {
			slog.Error("unable to determine if mirror is cdc", slog.Any("error", err))
			return &protos.MirrorStatusResponse{
				FlowJobName:      req.FlowJobName,
				CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
				ErrorMessage:     "unable to determine if mirror" + req.FlowJobName + "is of type CDC.",
				Ok:               false,
			}, nil
		}
		if cdcFlow {
			cdcStatus, err := h.cdcFlowStatus(ctx, req)
			if err != nil {
				slog.Error("unable to obtain CDC information for mirror", slog.Any("error", err))
				return &protos.MirrorStatusResponse{
					FlowJobName:      req.FlowJobName,
					CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
					ErrorMessage:     "unable to obtain CDC information for mirror " + req.FlowJobName,
					Ok:               false,
				}, nil
			}

			return &protos.MirrorStatusResponse{
				FlowJobName: req.FlowJobName,
				Status: &protos.MirrorStatusResponse_CdcStatus{
					CdcStatus: cdcStatus,
				},
				CurrentFlowState: currState,
				Ok:               true,
				CreatedAt:        timestamppb.New(*createdAt),
			}, nil
		} else {
			qrepStatus, err := h.qrepFlowStatus(ctx, req)
			if err != nil {
				slog.Error("unable to obtain qrep information for mirror", slog.Any("error", err))
				return &protos.MirrorStatusResponse{
					FlowJobName:      req.FlowJobName,
					CurrentFlowState: protos.FlowStatus_STATUS_UNKNOWN,
					ErrorMessage:     "unable to obtain snapshot information for mirror " + req.FlowJobName,
					Ok:               false,
				}, nil
			}

			return &protos.MirrorStatusResponse{
				FlowJobName: req.FlowJobName,
				Status: &protos.MirrorStatusResponse_QrepStatus{
					QrepStatus: qrepStatus,
				},
				CurrentFlowState: currState,
				Ok:               true,
				CreatedAt:        timestamppb.New(*createdAt),
			}, nil
		}
	}

	return &protos.MirrorStatusResponse{
		FlowJobName:      req.FlowJobName,
		CurrentFlowState: currState,
		Ok:               true,
		CreatedAt:        timestamppb.New(*createdAt),
	}, nil
}

func (h *FlowRequestHandler) cdcFlowStatus(
	ctx context.Context,
	req *protos.MirrorStatusRequest,
) (*protos.CDCMirrorStatus, error) {
	slog.Info("CDC mirror status endpoint called", slog.String(string(shared.FlowNameKey), req.FlowJobName))
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("unable to query flow config from catalog", slog.Any("error", err))
		return nil, err
	}
	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("unable to get the workflow ID of mirror", slog.Any("error", err))
		return nil, err
	}
	state, err := h.getCDCWorkflowState(ctx, workflowID)
	if err != nil {
		slog.Error("unable to get the state of mirror", slog.Any("error", err))
		return nil, err
	}

	// patching config to show latest values from state
	if state.SyncFlowOptions != nil {
		config.IdleTimeoutSeconds = state.SyncFlowOptions.IdleTimeoutSeconds
		config.MaxBatchSize = state.SyncFlowOptions.BatchSize
		config.TableMappings = state.SyncFlowOptions.TableMappings
	}

	srcType, err := connectors.LoadPeerType(ctx, h.pool, config.SourceName)
	if err != nil {
		slog.Error("unable to load source peer type", slog.Any("error", err))
		return nil, err
	}
	dstType, err := connectors.LoadPeerType(ctx, h.pool, config.DestinationName)
	if err != nil {
		slog.Error("unable to load destination peer type", slog.Any("error", err))
		return nil, err
	}

	cloneStatuses, err := h.cloneTableSummary(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("unable to query clone table summary", slog.Any("error", err))
		return nil, err
	}

	cdcBatches, err := h.getCdcBatches(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	return &protos.CDCMirrorStatus{
		Config:          config,
		SourceType:      srcType,
		DestinationType: dstType,
		SnapshotStatus: &protos.SnapshotStatus{
			Clones: cloneStatuses,
		},
		CdcBatches: cdcBatches,
	}, nil
}

func (h *FlowRequestHandler) cloneTableSummary(
	ctx context.Context,
	parentMirrorName string,
) ([]*protos.CloneTableSummary, error) {
	q := `
	SELECT
		distinct qr.flow_name,
		qr.destination_table,
		qr.source_table,
		qr.start_time AS StartTime,
		qr.fetch_complete as FetchCompleted,
		qr.consolidate_complete as ConsolidateCompleted,
		COUNT(CASE WHEN qp.flow_name IS NOT NULL THEN 1 END) AS NumPartitionsTotal,
		COUNT(CASE WHEN qp.end_time IS NOT NULL THEN 1 END) AS NumPartitionsCompleted,
		SUM(qp.rows_in_partition) FILTER (WHERE qp.end_time IS NOT NULL) AS NumRowsSynced,
		AVG(EXTRACT(EPOCH FROM (qp.end_time - qp.start_time)) * 1000) FILTER (WHERE qp.end_time IS NOT NULL) AS AvgTimePerPartitionMs
	FROM peerdb_stats.qrep_partitions qp
	RIGHT JOIN peerdb_stats.qrep_runs qr ON qp.flow_name = qr.flow_name
	WHERE qr.parent_mirror_name = $1
	GROUP BY qr.flow_name, qr.destination_table, qr.source_table, qr.start_time, qr.fetch_complete, qr.consolidate_complete;
	`
	var flowName pgtype.Text
	var destinationTable pgtype.Text
	var sourceTable pgtype.Text
	var fetchCompleted pgtype.Bool
	var consolidateCompleted pgtype.Bool
	var startTime pgtype.Timestamp
	var numPartitionsTotal pgtype.Int8
	var numPartitionsCompleted pgtype.Int8
	var numRowsSynced pgtype.Int8
	var avgTimePerPartitionMs pgtype.Float8

	rows, err := h.pool.Query(ctx, q, parentMirrorName)
	if err != nil {
		slog.Error("unable to query initial load partition",
			slog.String(string(shared.FlowNameKey), parentMirrorName), slog.Any("error", err))
		return nil, fmt.Errorf("unable to query initial load partition - %s: %w", parentMirrorName, err)
	}

	defer rows.Close()

	cloneStatuses := []*protos.CloneTableSummary{}
	for rows.Next() {
		if err := rows.Scan(
			&flowName,
			&destinationTable,
			&sourceTable,
			&startTime,
			&fetchCompleted,
			&consolidateCompleted,
			&numPartitionsTotal,
			&numPartitionsCompleted,
			&numRowsSynced,
			&avgTimePerPartitionMs,
		); err != nil {
			return nil, fmt.Errorf("unable to scan initial load partition - %s: %w", parentMirrorName, err)
		}

		var res protos.CloneTableSummary

		if flowName.Valid {
			res.FlowJobName = flowName.String
		}

		if destinationTable.Valid {
			res.TableName = destinationTable.String
		}

		if sourceTable.Valid {
			res.SourceTable = sourceTable.String
		}

		if startTime.Valid {
			res.StartTime = timestamppb.New(startTime.Time)
		}

		if fetchCompleted.Valid {
			res.FetchCompleted = fetchCompleted.Bool
		}

		if consolidateCompleted.Valid {
			res.ConsolidateCompleted = consolidateCompleted.Bool
		}

		if numPartitionsTotal.Valid {
			res.NumPartitionsTotal = int32(numPartitionsTotal.Int64)
		}

		if numPartitionsCompleted.Valid {
			res.NumPartitionsCompleted = int32(numPartitionsCompleted.Int64)
		}

		if numRowsSynced.Valid {
			res.NumRowsSynced = numRowsSynced.Int64
		}

		if avgTimePerPartitionMs.Valid {
			res.AvgTimePerPartitionMs = int64(avgTimePerPartitionMs.Float64)
		}

		res.MirrorName = parentMirrorName

		cloneStatuses = append(cloneStatuses, &res)
	}
	return cloneStatuses, nil
}

func (h *FlowRequestHandler) qrepFlowStatus(
	ctx context.Context,
	req *protos.MirrorStatusRequest,
) (*protos.QRepMirrorStatus, error) {
	slog.Info("QRep Flow status endpoint called", slog.String(string(shared.FlowNameKey), req.FlowJobName))
	partitionStatuses, err := h.getPartitionStatuses(ctx, req.FlowJobName)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query qrep partition - %s: %s", req.FlowJobName, err.Error()))
		return nil, err
	}

	return &protos.QRepMirrorStatus{
		// The clone table jobs that are children of the CDC snapshot flow
		// do not have a config entry, so allow this to be nil.
		Partitions: partitionStatuses,
	}, nil
}

func (h *FlowRequestHandler) getPartitionStatuses(
	ctx context.Context,
	flowJobName string,
) ([]*protos.PartitionStatus, error) {
	q := "SELECT partition_uuid,start_time,end_time,rows_in_partition,rows_synced FROM peerdb_stats.qrep_partitions WHERE flow_name=$1"
	rows, err := h.pool.Query(ctx, q, flowJobName)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query qrep partition - %s: %s", flowJobName, err.Error()))
		return nil, fmt.Errorf("unable to query qrep partition - %s: %w", flowJobName, err)
	}

	defer rows.Close()

	res := []*protos.PartitionStatus{}
	var partitionId pgtype.Text
	var startTime pgtype.Timestamp
	var endTime pgtype.Timestamp
	var numRowsInPartition pgtype.Int8
	var numRowsSynced pgtype.Int8

	for rows.Next() {
		if err := rows.Scan(&partitionId, &startTime, &endTime, &numRowsInPartition, &numRowsSynced); err != nil {
			slog.Error(fmt.Sprintf("unable to scan qrep partition - %s: %s", flowJobName, err.Error()))
			return nil, fmt.Errorf("unable to scan qrep partition - %s: %w", flowJobName, err)
		}

		partitionStatus := &protos.PartitionStatus{}

		if partitionId.Valid {
			partitionStatus.PartitionId = partitionId.String
		}
		if startTime.Valid {
			partitionStatus.StartTime = timestamppb.New(startTime.Time)
		}
		if endTime.Valid {
			partitionStatus.EndTime = timestamppb.New(endTime.Time)
		}
		if numRowsInPartition.Valid {
			partitionStatus.RowsInPartition = numRowsInPartition.Int64
		}
		if numRowsSynced.Valid {
			partitionStatus.RowsSynced = numRowsSynced.Int64
		}

		res = append(res, partitionStatus)
	}

	return res, nil
}

func (h *FlowRequestHandler) getFlowConfigFromCatalog(
	ctx context.Context,
	flowJobName string,
) (*protos.FlowConnectionConfigs, error) {
	var configBytes sql.RawBytes
	err := h.pool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1", flowJobName).Scan(&configBytes)
	if err != nil {
		slog.Error("unable to query flow config from catalog", slog.Any("error", err))
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	var config protos.FlowConnectionConfigs
	err = proto.Unmarshal(configBytes, &config)
	if err != nil {
		slog.Error("unable to unmarshal flow config", slog.Any("error", err))
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &config, nil
}

func (h *FlowRequestHandler) isCDCFlow(ctx context.Context, flowJobName string) (bool, error) {
	var isCdc bool
	err := h.pool.QueryRow(ctx, "SELECT exists(SELECT * FROM flows WHERE name=$1 and coalesce(query_string, '')='')",
		flowJobName).Scan(&isCdc)
	if err != nil {
		slog.Error("unable to query flow", slog.Any("error", err))
		return false, fmt.Errorf("unable to query flow: %w", err)
	}
	return isCdc, nil
}

func (h *FlowRequestHandler) getWorkflowStatus(ctx context.Context, workflowID string) (protos.FlowStatus, error) {
	res, err := h.temporalClient.QueryWorkflow(ctx, workflowID, "", shared.FlowStatusQuery)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get status in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	var state protos.FlowStatus
	err = res.Get(&state)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get status in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	return state, nil
}

func (h *FlowRequestHandler) getCDCWorkflowState(ctx context.Context,
	workflowID string,
) (*peerflow.CDCFlowWorkflowState, error) {
	res, err := h.temporalClient.QueryWorkflow(ctx, workflowID, "", shared.CDCFlowStateQuery)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get state in workflow with ID %s: %s", workflowID, err.Error()))
		return nil,
			fmt.Errorf("failed to get state in workflow with ID %s: %w", workflowID, err)
	}
	var state peerflow.CDCFlowWorkflowState
	if err := res.Get(&state); err != nil {
		slog.Error(fmt.Sprintf("failed to get state in workflow with ID %s: %s", workflowID, err.Error()))
		return nil,
			fmt.Errorf("failed to get state in workflow with ID %s: %w", workflowID, err)
	}
	return &state, nil
}

func (h *FlowRequestHandler) getMirrorCreatedAt(ctx context.Context, flowJobName string) (*time.Time, error) {
	var createdAt pgtype.Timestamp
	err := h.pool.QueryRow(ctx, "SELECT created_at FROM flows WHERE name=$1", flowJobName).Scan(&createdAt)
	if err != nil {
		slog.Error("unable to query flow", slog.Any("error", err))
		return nil, fmt.Errorf("unable to query flow: %w", err)
	}

	if !createdAt.Valid {
		return nil, fmt.Errorf("unable to get created_at for flow %s", flowJobName)
	}
	return &createdAt.Time, nil
}

func (h *FlowRequestHandler) getCdcBatches(ctx context.Context, flowJobName string) ([]*protos.CDCBatch, error) {
	q := `SELECT DISTINCT ON(batch_id) batch_id,start_time,end_time,rows_in_batch,batch_start_lsn,batch_end_lsn FROM peerdb_stats.cdc_batches
	  WHERE flow_name=$1 AND start_time IS NOT NULL ORDER BY batch_id DESC, start_time DESC`
	rows, err := h.pool.Query(ctx, q, flowJobName)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query cdc batches - %s: %s", flowJobName, err.Error()))
		return nil, fmt.Errorf("unable to query cdc batches - %s: %w", flowJobName, err)
	}

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.CDCBatch, error) {
		var batchID pgtype.Int8
		var startTime pgtype.Timestamp
		var endTime pgtype.Timestamp
		var numRows pgtype.Int8
		var startLSN pgtype.Numeric
		var endLSN pgtype.Numeric
		if err := rows.Scan(&batchID, &startTime, &endTime, &numRows, &startLSN, &endLSN); err != nil {
			slog.Error(fmt.Sprintf("unable to scan cdc batches - %s: %s", flowJobName, err.Error()))
			return nil, fmt.Errorf("unable to scan cdc batches - %s: %w", flowJobName, err)
		}

		var batch protos.CDCBatch

		if batchID.Valid {
			batch.BatchId = batchID.Int64
		}
		if startTime.Valid {
			batch.StartTime = timestamppb.New(startTime.Time)
		}
		if endTime.Valid {
			batch.EndTime = timestamppb.New(endTime.Time)
		}
		if numRows.Valid {
			batch.NumRows = numRows.Int64
		}
		if startLSN.Valid {
			batch.StartLsn = startLSN.Int.Int64()
		}
		if endLSN.Valid {
			batch.EndLsn = endLSN.Int.Int64()
		}

		return &batch, nil
	})
}

func (h *FlowRequestHandler) CDCTableTotalCounts(
	ctx context.Context,
	req *protos.CDCTableTotalCountsRequest,
) (*protos.CDCTableTotalCountsResponse, error) {
	rows, err := h.pool.Query(ctx, `select destination_table_name,
			sum(insert_count) inserts,
			sum(update_count) updates,
			sum(delete_count) deletes
		from peerdb_stats.cdc_batch_table
		where flow_name=$1
		group by destination_table_name`, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	var totalCount protos.CDCRowCounts
	tableCounts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.CDCTableRowCounts, error) {
		tableCount := &protos.CDCTableRowCounts{
			Counts: &protos.CDCRowCounts{},
		}
		err := row.Scan(&tableCount.TableName, &tableCount.Counts.InsertsCount,
			&tableCount.Counts.UpdatesCount, &tableCount.Counts.DeletesCount)
		tableCount.Counts.TotalCount = tableCount.Counts.InsertsCount + tableCount.Counts.UpdatesCount + tableCount.Counts.DeletesCount

		totalCount.TotalCount += tableCount.Counts.TotalCount
		totalCount.InsertsCount += tableCount.Counts.InsertsCount
		totalCount.UpdatesCount += tableCount.Counts.UpdatesCount
		totalCount.DeletesCount += tableCount.Counts.DeletesCount
		return tableCount, err
	})
	if err != nil {
		return nil, err
	}
	return &protos.CDCTableTotalCountsResponse{TotalData: &totalCount, TablesData: tableCounts}, nil
}

func (h *FlowRequestHandler) ListMirrorNames(
	ctx context.Context,
	req *protos.ListMirrorNamesRequest,
) (*protos.ListMirrorNamesResponse, error) {
	// selects from flow_errors to still list dropped mirrors
	rows, err := h.pool.Query(ctx, `select distinct flow_name
		from peerdb_stats.flow_errors
		where flow_name not like 'clone_%'
		order by flow_name`)
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows[string](rows, pgx.RowTo)
	if err != nil {
		return nil, err
	}
	return &protos.ListMirrorNamesResponse{
		Names: names,
	}, nil
}

func (h *FlowRequestHandler) ListMirrorLogs(
	ctx context.Context,
	req *protos.ListMirrorLogsRequest,
) (*protos.ListMirrorLogsResponse, error) {
	whereExprs := make([]string, 0, 2)
	whereArgs := make([]interface{}, 0, 2)
	if req.FlowJobName != "" {
		whereArgs = append(whereArgs, req.FlowJobName)
		whereExprs = append(whereExprs, "position($1 in flow_name) > 0")
	}

	if req.Level != "" && req.Level != "all" {
		whereArgs = append(whereArgs, req.Level)
		whereExprs = append(whereExprs, fmt.Sprintf("error_type = $%d", len(whereArgs)))
	}

	var whereClause string
	if len(whereExprs) != 0 {
		whereClause = " WHERE " + strings.Join(whereExprs, " AND ")
	}

	skip := (req.Page - 1) * req.NumPerPage
	rows, err := h.pool.Query(ctx, fmt.Sprintf(`select flow_name, error_message, error_type, error_timestamp
	from peerdb_stats.flow_errors %s
	order by error_timestamp desc
	limit %d offset %d`, whereClause, req.NumPerPage, skip), whereArgs...)
	if err != nil {
		return nil, err
	}
	mirrorErrors, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.MirrorLog, error) {
		var log protos.MirrorLog
		var errorTimestamp time.Time
		if err := rows.Scan(&log.FlowName, &log.ErrorMessage, &log.ErrorType, &errorTimestamp); err != nil {
			return nil, err
		}
		log.ErrorTimestamp = float64(errorTimestamp.UnixMilli())
		return &log, nil
	})
	if err != nil {
		return nil, err
	}

	var total int32
	if err := h.pool.QueryRow(ctx, "select count(*) from peerdb_stats.flow_errors"+whereClause, whereArgs...).Scan(&total); err != nil {
		return nil, err
	}

	return &protos.ListMirrorLogsResponse{
		Errors: mirrorErrors,
		Total:  total,
	}, nil
}
