package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func (h *FlowRequestHandler) flowExists(ctx context.Context, flowName string) (bool, error) {
	var exists bool
	if err := h.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM flows WHERE name = $1)", flowName).Scan(&exists); err != nil {
		slog.ErrorContext(ctx, "error checking if flow exists", slog.Any("error", err))
		return false, err
	}

	slog.InfoContext(ctx, fmt.Sprintf("flow %s exists: %t", flowName, exists))
	return exists, nil
}

func (h *FlowRequestHandler) CreateOrReplaceFlowTags(
	ctx context.Context,
	in *protos.CreateOrReplaceFlowTagsRequest,
) (*protos.CreateOrReplaceFlowTagsResponse, error) {
	flowName := in.FlowName

	if exists, err := h.flowExists(ctx, flowName); err != nil {
		return nil, exceptions.NewInternalApiError(err.Error())
	} else if !exists {
		slog.ErrorContext(ctx, "flow does not exist", slog.String("flow_name", flowName))
		return nil, exceptions.NewNotFoundApiError(fmt.Sprintf("flow %s does not exist", flowName))
	}

	tags := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		tags[tag.Key] = tag.Value
	}

	if _, err := h.pool.Exec(ctx, "UPDATE flows SET tags=$1, updated_at=now() WHERE name=$2", tags, flowName); err != nil {
		slog.ErrorContext(ctx, "error updating flow tags", slog.Any("error", err))
		return nil, exceptions.NewInternalApiError(err.Error())
	}

	return &protos.CreateOrReplaceFlowTagsResponse{
		FlowName: flowName,
	}, nil
}

func (h *FlowRequestHandler) GetFlowTags(ctx context.Context, in *protos.GetFlowTagsRequest) (*protos.GetFlowTagsResponse, error) {
	flowName := in.FlowName

	if exists, err := h.flowExists(ctx, flowName); err != nil {
		return nil, exceptions.NewInternalApiError(err.Error())
	} else if !exists {
		slog.ErrorContext(ctx, "flow does not exist", slog.String("flow_name", flowName))
		return nil, exceptions.NewNotFoundApiError(fmt.Sprintf("flow %s does not exist", flowName))
	}

	tags, err := alerting.GetTags(ctx, h.pool, flowName)
	if err != nil {
		slog.ErrorContext(ctx, "error getting flow tags", slog.Any("error", err))
		return nil, exceptions.NewInternalApiError(fmt.Sprintf("error getting flow tags: %v", err))
	}

	protosTags := make([]*protos.FlowTag, 0, len(tags))
	for key, value := range tags {
		protosTags = append(protosTags, &protos.FlowTag{Key: key, Value: value})
	}

	return &protos.GetFlowTagsResponse{
		FlowName: flowName,
		Tags:     protosTags,
	}, nil
}
