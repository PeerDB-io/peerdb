package cmd

import (
	"context"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

func (h *FlowRequestHandler) GetDynamicSettings(
	ctx context.Context,
	req *protos.GetDynamicSettingsRequest,
) (*protos.GetDynamicSettingsResponse, error) {
	rows, err := h.pool.Query(ctx, "select config_name,config_value from dynamic_settings")
	if err != nil {
		slog.Error("[GetDynamicConfigs]: failed to query settings", slog.Any("error", err))
		return nil, err
	}

	settings := slices.Clone(peerdbenv.DynamicSettings[:])
	var name string
	var value string
	if _, err := pgx.ForEachRow(rows, []any{&name, &value}, func() error {
		for i, setting := range settings {
			if setting.Name == name {
				settings[i] = shared.CloneProto(setting)
				settings[i].Value = &value
			}
		}
		return nil
	}); err != nil {
		slog.Error("[GetDynamicConfigs]: failed to collect rows", slog.Any("error", err))
		return nil, err
	}

	return &protos.GetDynamicSettingsResponse{Settings: settings}, nil
}

func (h *FlowRequestHandler) PostDynamicSetting(
	ctx context.Context,
	req *protos.PostDynamicSettingRequest,
) (*protos.PostDynamicSettingResponse, error) {
	_, err := h.pool.Exec(ctx, `insert into dynamic_settings (config_name, config_value) values ($2, $1)
		on conflict (config_name) do update set config_value = $1`, req.Value, req.Name)
	if err != nil {
		slog.Error("[PostDynamicConfig]: failed to execute update setting", slog.Any("error", err))
		return nil, err
	}
	return &protos.PostDynamicSettingResponse{}, nil
}
