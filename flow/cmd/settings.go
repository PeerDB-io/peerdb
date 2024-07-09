package cmd

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

func (h *FlowRequestHandler) GetDynamicSettings(
	ctx context.Context,
	req *protos.GetDynamicSettingsRequest,
) (*protos.GetDynamicSettingsResponse, error) {
	rows, err := h.pool.Query(
		ctx,
		"select config_name,config_value,config_default_value,config_description,config_value_type,config_apply_mode from dynamic_settings",
	)
	if err != nil {
		slog.Error("[GetDynamicConfigs]: failed to query settings", slog.Any("error", err))
		return nil, err
	}

	settings, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.DynamicSetting, error) {
		setting := &protos.DynamicSetting{}
		err := row.Scan(&setting.Name, &setting.Value, &setting.DefaultValue, &setting.Description, &setting.ValueType, &setting.ApplyMode)
		return setting, err
	})
	if err != nil {
		slog.Error("[GetDynamicConfigs]: failed to collect rows", slog.Any("error", err))
		return nil, err
	}

	return &protos.GetDynamicSettingsResponse{Settings: settings}, nil
}

func (h *FlowRequestHandler) PostDynamicSetting(
	ctx context.Context,
	req *protos.PostDynamicSettingRequest,
) (*protos.PostDynamicSettingResponse, error) {
	_, err := h.pool.Exec(ctx, "update dynamic_settings set config_value = $1 where config_name = $2", req.Value, req.Name)
	if err != nil {
		slog.Error("[PostDynamicConfig]: failed to execute update setting", slog.Any("error", err))
		return nil, err
	}
	return &protos.PostDynamicSettingResponse{}, nil
}
