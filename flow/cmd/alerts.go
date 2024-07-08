package cmd

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) GetAlertConfigs(ctx context.Context, req *protos.GetAlertConfigsRequest) (*protos.GetAlertConfigsResponse, error) {
	rows, err := h.pool.Query(ctx, "select id, service_type, service_config from peerdb_stats.alerting_config")
	if err != nil {
		return nil, err
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.AlertConfig, error) {
		config := &protos.AlertConfig{}
		err := row.Scan(&config.Id, &config.ServiceType, &config.ServiceConfig)
		return config, err
	})
	if err != nil {
		return nil, err
	}

	return &protos.GetAlertConfigsResponse{Configs: configs}, nil
}

func (h *FlowRequestHandler) PostAlertConfig(ctx context.Context, req *protos.PostAlertConfigRequest) (*protos.PostAlertConfigResponse, error) {
	if req.Id == -1 {
		var id int32
		if err := h.pool.QueryRow(
			ctx,
			"insert into peerdb_stats.alerting_config (service_type, service_config) values ($1, $2) returning id",
			req.ServiceType,
			req.ServiceConfig,
		).Scan(&id); err != nil {
			return nil, err
		}
		return &protos.PostAlertConfigResponse{Id: id}, nil
	} else if _, err := h.pool.Exec(
		ctx,
		"update peerdb_stats.alerting_config set service_type = $1, service_config = $2 where id = $3",
		req.ServiceType,
		req.ServiceConfig,
		req.Id,
	); err != nil {
		return nil, err
	}
	return &protos.PostAlertConfigResponse{Id: req.Id}, nil
}

func (h *FlowRequestHandler) DeleteAlertConfig(
	ctx context.Context,
	req *protos.DeleteAlertConfigRequest,
) (*protos.DeleteAlertConfigResponse, error) {
	if _, err := h.pool.Exec(ctx, "delete from peerdb_stats.alerting_config where id = $1", req.Id); err != nil {
		return nil, err
	}
	return &protos.DeleteAlertConfigResponse{}, nil
}
