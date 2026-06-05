package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// secretFieldsByServiceType lists JSON keys whose values must never be
// returned to API clients. Keep this in sync with the alert sender configs
// in flow/alerting.
var secretFieldsByServiceType = map[string][]string{
	"slack": {"auth_token"},
}

// redactServiceConfig returns serviceConfig with all secret fields for the
// given service type replaced by the empty string. Unknown service types
// are passed through unchanged.
func redactServiceConfig(serviceType string, serviceConfig []byte) ([]byte, error) {
	secretFields, ok := secretFieldsByServiceType[serviceType]
	if !ok {
		return serviceConfig, nil
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(serviceConfig, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service config for redaction: %w", err)
	}
	empty, _ := json.Marshal("")
	for _, field := range secretFields {
		if _, present := cfg[field]; present {
			cfg[field] = empty
		}
	}
	return json.Marshal(cfg)
}

// mergePreservingSecrets returns submitted with any empty secret fields
// populated from existing, so that clients can omit a secret to keep its
// stored value (clients never receive the value, see redactServiceConfig).
func mergePreservingSecrets(serviceType string, submitted, existing []byte) ([]byte, error) {
	secretFields, ok := secretFieldsByServiceType[serviceType]
	if !ok {
		return submitted, nil
	}
	var submittedMap map[string]json.RawMessage
	if err := json.Unmarshal(submitted, &submittedMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal submitted service config: %w", err)
	}
	var existingMap map[string]json.RawMessage
	if err := json.Unmarshal(existing, &existingMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal existing service config: %w", err)
	}
	changed := false
	for _, field := range secretFields {
		submittedVal, submittedHas := submittedMap[field]
		if !submittedHas || isEmptyJSONString(submittedVal) {
			if existingVal, existingHas := existingMap[field]; existingHas {
				submittedMap[field] = existingVal
				changed = true
			}
		}
	}
	if !changed {
		return submitted, nil
	}
	return json.Marshal(submittedMap)
}

func isEmptyJSONString(raw json.RawMessage) bool {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return false
	}
	return s == ""
}

func (h *FlowRequestHandler) GetAlertConfigs(
	ctx context.Context,
	req *protos.GetAlertConfigsRequest,
) (*protos.GetAlertConfigsResponse, APIError) {
	rows, err := h.pool.Query(ctx, "SELECT id,service_type,service_config,enc_key_id,alert_for_mirrors from peerdb_stats.alerting_config")
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get alert configs: %w", err))
	}

	configs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.AlertConfig, error) {
		var serviceConfigPayload []byte
		var encKeyID string
		config := &protos.AlertConfig{}
		if err := row.Scan(&config.Id, &config.ServiceType, &serviceConfigPayload, &encKeyID, &config.AlertForMirrors); err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to scan alert config: %w", err))
		}
		serviceConfig, err := internal.Decrypt(ctx, encKeyID, serviceConfigPayload)
		if err != nil {
			return nil, NewInternalApiError(fmt.Errorf("failed to decrypt alert config: %w", err))
		}
		redacted, err := redactServiceConfig(config.ServiceType, serviceConfig)
		if err != nil {
			return nil, NewInternalApiError(err)
		}
		config.ServiceConfig = string(redacted)
		return config, nil
	})
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to collect alert configs: %w", err))
	}

	return &protos.GetAlertConfigsResponse{Configs: configs}, nil
}

func (h *FlowRequestHandler) PostAlertConfig(
	ctx context.Context,
	req *protos.PostAlertConfigRequest,
) (*protos.PostAlertConfigResponse, APIError) {
	key, err := internal.PeerDBCurrentEncKey(ctx)
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to get current enc key: %w", err))
	}

	// On update, fill empty secret fields from the stored config so callers
	// can omit unchanged secrets (the API never returns them — see
	// redactServiceConfig).
	serviceConfigPlaintext := req.Config.ServiceConfig
	if req.Config.Id != -1 {
		var existingPayload []byte
		var existingEncKeyID string
		err := h.pool.QueryRow(ctx,
			"SELECT service_config, enc_key_id FROM peerdb_stats.alerting_config WHERE id = $1",
			req.Config.Id,
		).Scan(&existingPayload, &existingEncKeyID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, NewInternalApiError(fmt.Errorf("failed to load existing alert config: %w", err))
		}
		if err == nil {
			existingPlaintext, err := internal.Decrypt(ctx, existingEncKeyID, existingPayload)
			if err != nil {
				return nil, NewInternalApiError(fmt.Errorf("failed to decrypt existing alert config: %w", err))
			}
			merged, err := mergePreservingSecrets(req.Config.ServiceType,
				shared.UnsafeFastStringToReadOnlyBytes(req.Config.ServiceConfig), existingPlaintext)
			if err != nil {
				return nil, NewInternalApiError(err)
			}
			serviceConfigPlaintext = string(merged)
		}
	}

	serviceConfig, err := key.Encrypt(shared.UnsafeFastStringToReadOnlyBytes(serviceConfigPlaintext))
	if err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to encrypt alert config: %w", err))
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
			return nil, NewInternalApiError(fmt.Errorf("failed to insert alert config: %w", err))
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
		return nil, NewInternalApiError(fmt.Errorf("failed to update alert config: %w", err))
	}
	return &protos.PostAlertConfigResponse{Id: req.Config.Id}, nil
}

func (h *FlowRequestHandler) DeleteAlertConfig(
	ctx context.Context,
	req *protos.DeleteAlertConfigRequest,
) (*protos.DeleteAlertConfigResponse, APIError) {
	if _, err := h.pool.Exec(ctx, "delete from peerdb_stats.alerting_config where id = $1", req.Id); err != nil {
		return nil, NewInternalApiError(fmt.Errorf("failed to delete alert config: %w", err))
	}
	return &protos.DeleteAlertConfigResponse{}, nil
}
