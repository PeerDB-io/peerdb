package cmd

import (
	"context"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

func (h *FlowRequestHandler) GetDynamicSettings(
	ctx context.Context,
	req *protos.GetDynamicSettingsRequest,
) (*protos.GetDynamicSettingsResponse, error) {
	rows, err := h.pool.Query(ctx, "select config_name,config_value from dynamic_settings")
	if err != nil {
		slog.Error("[GetDynamicConfigs] failed to query settings", slog.Any("error", err))
		return nil, err
	}
	settings := slices.Clone(internal.DynamicSettings[:])
	var name string
	var value string
	if _, err := pgx.ForEachRow(rows, []any{&name, &value}, func() error {
		if idx, ok := internal.DynamicIndex[name]; ok {
			settings[idx] = proto.CloneOf(settings[idx])
			newValue := value // create a new string reference as value can be overwritten by the next iteration.
			settings[idx].Value = &newValue
		}
		return nil
	}); err != nil {
		slog.Error("[GetDynamicConfigs] failed to collect rows", slog.Any("error", err))
		return nil, err
	}

	if internal.PeerDBOnlyClickHouseAllowed() {
		filteredSettings := make([]*protos.DynamicSetting, 0)
		for _, setting := range settings {
			if setting.TargetForSetting == protos.DynconfTarget_ALL ||
				setting.TargetForSetting == protos.DynconfTarget_CLICKHOUSE {
				filteredSettings = append(filteredSettings, setting)
			}
		}
		settings = filteredSettings
	}

	return &protos.GetDynamicSettingsResponse{Settings: settings}, nil
}

func (h *FlowRequestHandler) PostDynamicSetting(
	ctx context.Context,
	req *protos.PostDynamicSettingRequest,
) (*protos.PostDynamicSettingResponse, error) {
	err := internal.UpdateDynamicSetting(ctx, h.pool, req.Name, req.Value)
	if err != nil {
		slog.Error("[PostDynamicConfig] failed to execute update setting", slog.Any("error", err))
		return nil, err
	}
	return &protos.PostDynamicSettingResponse{}, nil
}
