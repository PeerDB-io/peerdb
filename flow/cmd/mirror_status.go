package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (h *FlowRequestHandler) MirrorStatus(
	ctx context.Context,
	req *protos.MirrorStatusRequest,
) (*protos.MirrorStatusResponse, error) {
	slog.Info("Mirror status endpoint called", slog.String(string(shared.FlowNameKey), req.FlowJobName))
	cdcFlow, err := h.isCDCFlow(ctx, req.FlowJobName)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query flow: %s", err.Error()))
		return &protos.MirrorStatusResponse{
			ErrorMessage: fmt.Sprintf("unable to query flow: %s", err.Error()),
		}, nil
	}

	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	currState, err := h.getWorkflowStatus(ctx, workflowID)
	if err != nil {
		return &protos.MirrorStatusResponse{
			ErrorMessage: fmt.Sprintf("unable to get flow state: %s", err.Error()),
		}, nil
	}

	if cdcFlow {
		cdcStatus, err := h.CDCFlowStatus(ctx, req)
		if err != nil {
			return &protos.MirrorStatusResponse{
				ErrorMessage: fmt.Sprintf("unable to query flow: %s", err.Error()),
			}, nil
		}

		return &protos.MirrorStatusResponse{
			FlowJobName: req.FlowJobName,
			Status: &protos.MirrorStatusResponse_CdcStatus{
				CdcStatus: cdcStatus,
			},
			CurrentFlowState: currState,
		}, nil
	} else {
		qrepStatus, err := h.QRepFlowStatus(ctx, req)
		if err != nil {
			return &protos.MirrorStatusResponse{
				ErrorMessage: fmt.Sprintf("unable to query flow: %s", err.Error()),
			}, nil
		}

		return &protos.MirrorStatusResponse{
			FlowJobName: req.FlowJobName,
			Status: &protos.MirrorStatusResponse_QrepStatus{
				QrepStatus: qrepStatus,
			},
			CurrentFlowState: currState,
		}, nil
	}
}

func (h *FlowRequestHandler) CDCFlowStatus(
	ctx context.Context,
	req *protos.MirrorStatusRequest,
) (*protos.CDCMirrorStatus, error) {
	slog.Info("CDC mirror status endpoint called", slog.String(string(shared.FlowNameKey), req.FlowJobName))
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	var initialCopyStatus *protos.SnapshotStatus

	cloneStatuses, err := h.cloneTableSummary(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	initialCopyStatus = &protos.SnapshotStatus{
		Clones: cloneStatuses,
	}

	return &protos.CDCMirrorStatus{
		Config:         config,
		SnapshotStatus: initialCopyStatus,
	}, nil
}

func (h *FlowRequestHandler) cloneTableSummary(
	ctx context.Context,
	flowJobName string,
) ([]*protos.CloneTableSummary, error) {
	q := `
	SELECT
		qp.flow_name,
		qr.config_proto,
		MIN(qp.start_time) AS StartTime,
		COUNT(*) AS NumPartitionsTotal,
		COUNT(CASE WHEN qp.end_time IS NOT NULL THEN 1 END) AS NumPartitionsCompleted,
		SUM(qp.rows_in_partition) FILTER (WHERE qp.end_time IS NOT NULL) AS NumRowsSynced,
		AVG(EXTRACT(EPOCH FROM (qp.end_time - qp.start_time)) * 1000) FILTER (WHERE qp.end_time IS NOT NULL) AS AvgTimePerPartitionMs
	FROM
		peerdb_stats.qrep_partitions qp
	JOIN
		peerdb_stats.qrep_runs qr
	ON
		qp.flow_name = qr.flow_name
	WHERE
		qp.flow_name ILIKE $1
	GROUP BY
		qp.flow_name, qr.config_proto;
	`

	var flowName pgtype.Text
	var configBytes []byte
	var startTime pgtype.Timestamp
	var numPartitionsTotal pgtype.Int8
	var numPartitionsCompleted pgtype.Int8
	var numRowsSynced pgtype.Int8
	var avgTimePerPartitionMs pgtype.Float8

	rows, err := h.pool.Query(ctx, q, "clone_"+flowJobName+"_%")
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query initial load partition - %s: %s", flowJobName, err.Error()))
		return nil, fmt.Errorf("unable to query initial load partition - %s: %w", flowJobName, err)
	}

	defer rows.Close()

	cloneStatuses := []*protos.CloneTableSummary{}
	for rows.Next() {
		if err := rows.Scan(
			&flowName,
			&configBytes,
			&startTime,
			&numPartitionsTotal,
			&numPartitionsCompleted,
			&numRowsSynced,
			&avgTimePerPartitionMs,
		); err != nil {
			return nil, fmt.Errorf("unable to scan initial load partition - %s: %w", flowJobName, err)
		}

		var res protos.CloneTableSummary

		if flowName.Valid {
			res.FlowJobName = flowName.String
		}
		if startTime.Valid {
			res.StartTime = timestamppb.New(startTime.Time)
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

		if configBytes != nil {
			var config protos.QRepConfig
			if err := proto.Unmarshal(configBytes, &config); err != nil {
				slog.Error(fmt.Sprintf("unable to unmarshal config: %s", err.Error()))
				return nil, fmt.Errorf("unable to unmarshal config: %w", err)
			}
			res.TableName = config.DestinationTableIdentifier
		}

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
		Config:     h.getQRepConfigFromCatalog(ctx, req.FlowJobName),
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
	var err error
	var config protos.FlowConnectionConfigs

	err = h.pool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1", flowJobName).Scan(&configBytes)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query flow config from catalog: %s", err.Error()))
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	err = proto.Unmarshal(configBytes, &config)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to unmarshal flow config: %s", err.Error()))
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &config, nil
}

func (h *FlowRequestHandler) getQRepConfigFromCatalog(ctx context.Context, flowJobName string) *protos.QRepConfig {
	var configBytes []byte
	var config protos.QRepConfig

	queryInfos := []struct {
		Query   string
		Warning string
	}{
		{
			Query:   "SELECT config_proto FROM flows WHERE name = $1",
			Warning: "unable to query qrep config from catalog",
		},
		{
			Query:   "SELECT config_proto FROM peerdb_stats.qrep_runs WHERE flow_name = $1",
			Warning: "unable to query qrep config from qrep_runs",
		},
	}

	// Iterate over queries and attempt to fetch the config
	for _, qInfo := range queryInfos {
		err := h.pool.QueryRow(ctx, qInfo.Query, flowJobName).Scan(&configBytes)
		if err == nil {
			break
		}
		slog.Warn(fmt.Sprintf("%s - %s: %s", qInfo.Warning, flowJobName, err.Error()))
	}

	// If no config was fetched, return nil
	if len(configBytes) == 0 {
		return nil
	}

	// Try unmarshaling
	if err := proto.Unmarshal(configBytes, &config); err != nil {
		slog.Warn(fmt.Sprintf("failed to unmarshal config for %s: %s", flowJobName, err.Error()))
		return nil
	}

	return &config
}

func (h *FlowRequestHandler) isCDCFlow(ctx context.Context, flowJobName string) (bool, error) {
	var query pgtype.Text
	err := h.pool.QueryRow(ctx, "SELECT query_string FROM flows WHERE name = $1", flowJobName).Scan(&query)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to query flow: %s", err.Error()))
		return false, fmt.Errorf("unable to query flow: %w", err)
	}

	if !query.Valid || len(query.String) == 0 {
		return true, nil
	}

	return false, nil
}

func (h *FlowRequestHandler) getWorkflowStatus(ctx context.Context, workflowID string) (protos.FlowStatus, error) {
	res, err := h.temporalClient.QueryWorkflow(ctx, workflowID, "", shared.FlowStatusQuery)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get state in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get state in workflow with ID %s: %w", workflowID, err)
	}
	var state protos.FlowStatus
	err = res.Get(&state)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to get state in workflow with ID %s: %s", workflowID, err.Error()))
		return protos.FlowStatus_STATUS_UNKNOWN,
			fmt.Errorf("failed to get state in workflow with ID %s: %w", workflowID, err)
	}
	return state, nil
}

func (h *FlowRequestHandler) updateWorkflowStatus(
	ctx context.Context,
	workflowID string,
	state protos.FlowStatus,
) error {
	_, err := h.temporalClient.UpdateWorkflow(ctx, workflowID, "", shared.FlowStatusUpdate, state)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to update state in workflow with ID %s: %s", workflowID, err.Error()))
		return fmt.Errorf("failed to update state in workflow with ID %s: %w", workflowID, err)
	}
	return nil
}
