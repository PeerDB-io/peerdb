package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
)

// alerting service, no cool name :(
type Alerter struct {
	catalogPool     *pgxpool.Pool
	telemetrySender telemetry.Sender
}

type AlertSenderConfig struct {
	Sender          AlertSender
	AlertForMirrors []string
	Id              int64
}

type AlertKeys struct {
	FlowName string
	PeerName string
	SlotName string
}

func (a *Alerter) registerSendersFromPool(ctx context.Context) ([]AlertSenderConfig, error) {
	rows, err := a.catalogPool.Query(ctx,
		`SELECT
			id,
			service_type,
			service_config,
			enc_key_id,
			alert_for_mirrors
		FROM peerdb_stats.alerting_config`)
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	keys := peerdbenv.PeerDBEncKeys()
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (AlertSenderConfig, error) {
		var alertSenderConfig AlertSenderConfig
		var serviceType ServiceType
		var serviceConfigEnc []byte
		var encKeyId string
		if err := row.Scan(&alertSenderConfig.Id, &serviceType, &serviceConfigEnc, &encKeyId,
			&alertSenderConfig.AlertForMirrors); err != nil {
			return alertSenderConfig, err
		}

		key, err := keys.Get(encKeyId)
		if err != nil {
			return alertSenderConfig, err
		}
		serviceConfig, err := key.Decrypt(serviceConfigEnc)
		if err != nil {
			return alertSenderConfig, err
		}

		switch serviceType {
		case SLACK:
			var slackServiceConfig slackAlertConfig
			if err := json.Unmarshal(serviceConfig, &slackServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}

			alertSenderConfig.Sender = newSlackAlertSender(&slackServiceConfig)
			return alertSenderConfig, nil
		case EMAIL:
			var replyToAddresses []string
			if replyToEnvString := strings.TrimSpace(
				peerdbenv.PeerDBAlertingEmailSenderReplyToAddresses()); replyToEnvString != "" {
				replyToAddresses = strings.Split(replyToEnvString, ",")
			}
			emailServiceConfig := EmailAlertSenderConfig{
				sourceEmail:          peerdbenv.PeerDBAlertingEmailSenderSourceEmail(),
				configurationSetName: peerdbenv.PeerDBAlertingEmailSenderConfigurationSet(),
				replyToAddresses:     replyToAddresses,
			}
			if emailServiceConfig.sourceEmail == "" {
				return alertSenderConfig, errors.New("missing sourceEmail for Email alerting service")
			}
			if err := json.Unmarshal(serviceConfig, &emailServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}
			var region *string
			if envRegion := peerdbenv.PeerDBAlertingEmailSenderRegion(); envRegion != "" {
				region = &envRegion
			}

			alertSender, alertSenderErr := NewEmailAlertSenderWithNewClient(ctx, region, &emailServiceConfig)
			if alertSenderErr != nil {
				return AlertSenderConfig{}, fmt.Errorf("failed to initialize email alerter: %w", alertSenderErr)
			}
			alertSenderConfig.Sender = alertSender

			return alertSenderConfig, nil
		default:
			return alertSenderConfig, fmt.Errorf("unknown service type: %s", serviceType)
		}
	})
}

// doesn't take care of closing pool, needs to be done externally.
func NewAlerter(ctx context.Context, catalogPool *pgxpool.Pool) *Alerter {
	if catalogPool == nil {
		panic("catalog pool is nil for Alerter")
	}
	snsTopic := peerdbenv.PeerDBTelemetryAWSSNSTopicArn()
	var snsMessageSender telemetry.Sender
	if snsTopic != "" {
		var err error
		snsMessageSender, err = telemetry.NewSNSMessageSenderWithNewClient(ctx, &telemetry.SNSMessageSenderConfig{
			Topic: snsTopic,
		})
		logger.LoggerFromCtx(ctx).Info("Successfully registered telemetry sender")
		if err != nil {
			panic(fmt.Sprintf("unable to setup telemetry is nil for Alerter %+v", err))
		}
	}
	return &Alerter{
		catalogPool:     catalogPool,
		telemetrySender: snsMessageSender,
	}
}

func (a *Alerter) AlertIfSlotLag(ctx context.Context, alertKeys *AlertKeys, slotInfo *protos.SlotInfo) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", peerdbenv.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold, err := peerdbenv.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
		return
	}
	// catalog cannot use default threshold to space alerts properly, use the lowest set threshold instead
	lowestSlotLagMBAlertThreshold := defaultSlotLagMBAlertThreshold
	for _, alertSender := range alertSenderConfigs {
		if alertSender.Sender.getSlotLagMBAlertThreshold() > 0 {
			lowestSlotLagMBAlertThreshold = min(lowestSlotLagMBAlertThreshold, alertSender.Sender.getSlotLagMBAlertThreshold())
		}
	}

	alertKey := fmt.Sprintf("%s Slot Lag Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	alertMessageTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!`, deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.LagInMb)

	if slotInfo.LagInMb > float32(lowestSlotLagMBAlertThreshold) {
		for _, alertSenderConfig := range alertSenderConfigs {
			if len(alertSenderConfig.AlertForMirrors) > 0 &&
				!slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
				continue
			}
			if a.checkAndAddAlertToCatalog(ctx,
				alertSenderConfig.Id, alertKey, fmt.Sprintf(alertMessageTemplate, lowestSlotLagMBAlertThreshold)) {
				if alertSenderConfig.Sender.getSlotLagMBAlertThreshold() > 0 {
					if slotInfo.LagInMb > float32(alertSenderConfig.Sender.getSlotLagMBAlertThreshold()) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, alertSenderConfig.Sender.getSlotLagMBAlertThreshold()))
					}
				} else {
					if slotInfo.LagInMb > float32(defaultSlotLagMBAlertThreshold) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, defaultSlotLagMBAlertThreshold))
					}
				}
			}
		}
	}
}

func (a *Alerter) AlertIfOpenConnections(ctx context.Context, alertKeys *AlertKeys,
	openConnections *protos.GetOpenConnectionsForUserResult,
) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to set Slack senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", peerdbenv.PeerDBDeploymentUID())
	}

	// same as with slot lag, use lowest threshold for catalog
	defaultOpenConnectionsThreshold, err := peerdbenv.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
		return
	}
	lowestOpenConnectionsThreshold := defaultOpenConnectionsThreshold
	for _, alertSender := range alertSenderConfigs {
		if alertSender.Sender.getOpenConnectionsAlertThreshold() > 0 {
			lowestOpenConnectionsThreshold = min(lowestOpenConnectionsThreshold,
				alertSender.Sender.getOpenConnectionsAlertThreshold())
		}
	}

	alertKey := fmt.Sprintf("%s Max Open Connections Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	alertMessageTemplate := fmt.Sprintf("%sOpen connections from PeerDB user `%s` on peer `%s`"+
		` has exceeded threshold size of %%d connections, currently at %d connections!`,
		deploymentUIDPrefix, openConnections.UserName, alertKeys.PeerName, openConnections.CurrentOpenConnections)

	if openConnections.CurrentOpenConnections > int64(lowestOpenConnectionsThreshold) {
		for _, alertSenderConfig := range alertSenderConfigs {
			if len(alertSenderConfig.AlertForMirrors) > 0 &&
				!slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
				continue
			}
			if a.checkAndAddAlertToCatalog(ctx,
				alertSenderConfig.Id, alertKey, fmt.Sprintf(alertMessageTemplate, lowestOpenConnectionsThreshold)) {
				if alertSenderConfig.Sender.getOpenConnectionsAlertThreshold() > 0 {
					if openConnections.CurrentOpenConnections > int64(alertSenderConfig.Sender.getOpenConnectionsAlertThreshold()) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, alertSenderConfig.Sender.getOpenConnectionsAlertThreshold()))
					}
				} else {
					if openConnections.CurrentOpenConnections > int64(defaultOpenConnectionsThreshold) {
						a.alertToProvider(ctx, alertSenderConfig, alertKey,
							fmt.Sprintf(alertMessageTemplate, defaultOpenConnectionsThreshold))
					}
				}
			}
		}
	}
}

func (a *Alerter) alertToProvider(ctx context.Context, alertSenderConfig AlertSenderConfig, alertKey string, alertMessage string) {
	err := alertSenderConfig.Sender.sendAlert(ctx, alertKey, alertMessage)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
		return
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertConfigId int64, alertKey string, alertMessage string) bool {
	dur, err := peerdbenv.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to get alerting gap duration from catalog", slog.Any("error", err))
		return false
	}
	if dur == 0 {
		logger.LoggerFromCtx(ctx).Warn("Alerting disabled via environment variable, returning")
		return false
	}

	row := a.catalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1 AND alert_config_id=$2
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey, alertConfigId)
	var createdTimestamp time.Time
	err = row.Scan(&createdTimestamp)
	if err != nil && err != pgx.ErrNoRows {
		logger.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("err", err))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		_, err = a.catalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message,alert_config_id) VALUES($1,$2,$3)",
			alertKey, alertMessage, alertConfigId)
		if err != nil {
			logger.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}

	logger.LoggerFromCtx(ctx).Info(
		fmt.Sprintf("Skipped sending alerts: last alert was sent at %s, which was >=%s ago",
			createdTimestamp.String(), dur.String()))
	return false
}

func (a *Alerter) sendTelemetryMessage(ctx context.Context, flowName string, more string, level telemetry.Level) {
	if a.telemetrySender != nil {
		details := fmt.Sprintf("[%s] %s", flowName, more)
		_, err := a.telemetrySender.SendMessage(ctx, details, details, telemetry.Attributes{
			Level:         level,
			DeploymentUID: peerdbenv.PeerDBDeploymentUID(),
			Tags:          []string{flowName, peerdbenv.PeerDBDeploymentUID()},
			Type:          flowName,
		})
		if err != nil {
			logger.LoggerFromCtx(ctx).Warn("failed to send message to telemetrySender", slog.Any("error", err))
			return
		}
	}
}

// Wrapper for different telemetry levels for non-flow events
func (a *Alerter) LogNonFlowInfo(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.LogNonFlowEvent(ctx, eventType, key, message, telemetry.INFO)
}

func (a *Alerter) LogNonFlowWarning(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.LogNonFlowEvent(ctx, eventType, key, message, telemetry.WARN)
}

func (a *Alerter) LogNonFlowError(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.LogNonFlowEvent(ctx, eventType, key, message, telemetry.ERROR)
}

func (a *Alerter) LogNonFlowCritical(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.LogNonFlowEvent(ctx, eventType, key, message, telemetry.CRITICAL)
}

func (a *Alerter) LogNonFlowEvent(ctx context.Context, eventType telemetry.EventType, key string, message string, level telemetry.Level) {
	a.sendTelemetryMessage(ctx, string(eventType)+":"+key, message, level)
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) {
	logger := logger.LoggerFromCtx(ctx)
	errorWithStack := fmt.Sprintf("%+v", err)
	logger.Error(err.Error(), slog.Any("stack", errorWithStack))
	_, err = a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, errorWithStack, "error")
	if err != nil {
		logger.Warn("failed to insert flow error", slog.Any("error", err))
		return
	}
	a.sendTelemetryMessage(ctx, flowName, errorWithStack, telemetry.ERROR)
}

func (a *Alerter) LogFlowEvent(ctx context.Context, flowName string, info string) {
	logger.LoggerFromCtx(ctx).Info(info)
	a.sendTelemetryMessage(ctx, flowName, info, telemetry.INFO)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, info string) {
	logger := logger.LoggerFromCtx(ctx)
	logger.Info(info)
	_, err := a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, info, "info")
	if err != nil {
		logger.Warn("failed to insert flow info", slog.Any("error", err))
		return
	}
}
