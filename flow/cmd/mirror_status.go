package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

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
			cdcStatus, err := h.CDCFlowStatus(ctx, req)
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
			}, nil
		} else {
			qrepStatus, err := h.QRepFlowStatus(ctx, req)
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
			}, nil
		}
	}

	return &protos.MirrorStatusResponse{
		FlowJobName:      req.FlowJobName,
		CurrentFlowState: currState,
		Ok:               true,
	}, nil
}

func (h *FlowRequestHandler) CDCFlowStatus(
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

	return &protos.CDCMirrorStatus{
		Config:          config,
		SourceType:      srcType,
		DestinationType: dstType,
		SnapshotStatus: &protos.SnapshotStatus{
			Clones: cloneStatuses,
		},
	}, nil
}

func (h *FlowRequestHandler) cloneTableSummary(
	ctx context.Context,
	mirrorName string,
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
	WHERE qr.flow_name ^@ ($1||regexp_replace(qr.destination_table, '[^a-zA-Z0-9_]', '_', 'g'))
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

	rows, err := h.pool.Query(ctx, q, fmt.Sprintf("clone_%s_", mirrorName))
	if err != nil {
		slog.Error("unable to query initial load partition",
			slog.String(string(shared.FlowNameKey), mirrorName), slog.Any("error", err))
		return nil, fmt.Errorf("unable to query initial load partition - %s: %w", mirrorName, err)
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
			return nil, fmt.Errorf("unable to scan initial load partition - %s: %w", mirrorName, err)
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

		res.MirrorName = mirrorName

		cloneStatuses = append(cloneStatuses, &res)
	}
	return cloneStatuses, nil
}

func (h *FlowRequestHandler) QRepFlowStatus(
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
	q := "SELECT start_time, end_time, rows_in_partition FROM peerdb_stats.qrep_partitions WHERE flow_name = $1"
	rows, err := h.pool.Query(ctx, q, flowJobName)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query qrep partition - %s: %s", flowJobName, err.Error()))
		return nil, fmt.Errorf("unable to query qrep partition - %s: %w", flowJobName, err)
	}

	defer rows.Close()

	res := []*protos.PartitionStatus{}
	var startTime pgtype.Timestamp
	var endTime pgtype.Timestamp
	var numRows pgtype.Int4

	for rows.Next() {
		if err := rows.Scan(&startTime, &endTime, &numRows); err != nil {
			slog.Error(fmt.Sprintf("unable to scan qrep partition - %s: %s", flowJobName, err.Error()))
			return nil, fmt.Errorf("unable to scan qrep partition - %s: %w", flowJobName, err)
		}

		partitionStatus := &protos.PartitionStatus{}

		if startTime.Valid {
			partitionStatus.StartTime = timestamppb.New(startTime.Time)
		}

		if endTime.Valid {
			partitionStatus.EndTime = timestamppb.New(endTime.Time)
		}

		if numRows.Valid {
			partitionStatus.NumRows = numRows.Int32
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
