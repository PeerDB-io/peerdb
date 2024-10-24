package cmd

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (h *FlowRequestHandler) GetAlertConfigs(ctx context.Context, req *protos.GetAlertConfigsRequest) (*protos.GetAlertConfigsResponse, error) {
	rows, err := h.pool.Query(ctx, "SELECT id,service_type,service_config,enc_key_id,alert_for_mirrors from peerdb_stats.alerting_config")
	if err != nil {
		return nil, err
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.AlertConfig, error) {
		var serviceConfigPayload []byte
		var encKeyID string
		config := &protos.AlertConfig{}
		if err := row.Scan(&config.Id, &config.ServiceType, &serviceConfigPayload, &encKeyID, &config.AlertForMirrors); err != nil {
			return nil, err
		}
		serviceConfig, err := peerdbenv.Decrypt(ctx, encKeyID, serviceConfigPayload)
		if err != nil {
			return nil, err
		}
		config.ServiceConfig = string(serviceConfig)
		return config, nil
	})
	if err != nil {
		return nil, err
	}

	return &protos.GetAlertConfigsResponse{Configs: configs}, nil
}

func (h *FlowRequestHandler) PostAlertConfig(ctx context.Context, req *protos.PostAlertConfigRequest) (*protos.PostAlertConfigResponse, error) {
	key, err := peerdbenv.PeerDBCurrentEncKey(ctx)
	if err != nil {
		return nil, err
	}
	serviceConfig, err := key.Encrypt(shared.UnsafeFastStringToReadOnlyBytes(req.Config.ServiceConfig))
	if err != nil {
		return nil, err
	}

	if req.Config.Id == -1 {
		var id int32
		if err := h.pool.QueryRow(
			ctx,
			`INSERT INTO peerdb_stats.alerting_config (
				service_type,
				service_config,
				enc_key_id,
				alert_for_mirrors
			) VALUES (
				$1,
				$2,
				$3,
				$4
			) RETURNING id`,
			req.Config.ServiceType,
			serviceConfig,
			key.ID,
			req.Config.AlertForMirrors,
		).Scan(&id); err != nil {
			return nil, err
		}
		return &protos.PostAlertConfigResponse{Id: id}, nil
	} else if _, err := h.pool.Exec(
		ctx,
		"update peerdb_stats.alerting_config set service_type = $1, service_config = $2, enc_key_id = $3, alert_for_mirrors = $4 where id = $5",
		req.Config.ServiceType,
		serviceConfig,
		key.ID,
		req.Config.AlertForMirrors,
		req.Config.Id,
	); err != nil {
		return nil, err
	}
	return &protos.PostAlertConfigResponse{Id: req.Config.Id}, nil
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
