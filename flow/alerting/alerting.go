package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/dynamicconf"
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

func (a *Alerter) registerSendersFromPool(ctx context.Context) ([]*slackAlertSender, error) {
	rows, err := a.catalogPool.Query(ctx,
		"SELECT service_type,service_config FROM peerdb_stats.alerting_config")
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	var slackAlertSenders []*slackAlertSender
	var serviceType, serviceConfig string
	_, err = pgx.ForEachRow(rows, []any{&serviceType, &serviceConfig}, func() error {
		switch serviceType {
		case "slack":
			var slackServiceConfig slackAlertConfig
			err = json.Unmarshal([]byte(serviceConfig), &slackServiceConfig)
			if err != nil {
				return fmt.Errorf("failed to unmarshal Slack service config: %w", err)
			}

			slackAlertSenders = append(slackAlertSenders, newSlackAlertSender(&slackServiceConfig))
		default:
			return fmt.Errorf("unknown service type: %s", serviceType)
		}
		return nil
	})

	return slackAlertSenders, nil
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

func (a *Alerter) AlertIfSlotLag(ctx context.Context, peerName string, slotInfo *protos.SlotInfo) {
	slackAlertSenders, err := a.registerSendersFromPool(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to set Slack senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", peerdbenv.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold := dynamicconf.PeerDBSlotLagMBAlertThreshold(ctx)
	// catalog cannot use default threshold to space alerts properly, use the lowest set threshold instead
	lowestSlotLagMBAlertThreshold := defaultSlotLagMBAlertThreshold
	for _, slackAlertSender := range slackAlertSenders {
		if slackAlertSender.slotLagMBAlertThreshold > 0 {
			lowestSlotLagMBAlertThreshold = min(lowestSlotLagMBAlertThreshold, slackAlertSender.slotLagMBAlertThreshold)
		}
	}

	alertKey := peerName + "-slot-lag-threshold-exceeded"
	alertMessageTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!
		cc: <!channel>`, deploymentUIDPrefix, slotInfo.SlotName, peerName, slotInfo.LagInMb)

	if slotInfo.LagInMb > float32(lowestSlotLagMBAlertThreshold) &&
		a.checkAndAddAlertToCatalog(ctx, alertKey, fmt.Sprintf(alertMessageTemplate, lowestSlotLagMBAlertThreshold)) {
		for _, slackAlertSender := range slackAlertSenders {
			if slackAlertSender.slotLagMBAlertThreshold > 0 {
				if slotInfo.LagInMb > float32(slackAlertSender.slotLagMBAlertThreshold) {
					a.alertToSlack(ctx, slackAlertSender, alertKey,
						fmt.Sprintf(alertMessageTemplate, slackAlertSender.slotLagMBAlertThreshold))
				}
			} else {
				if slotInfo.LagInMb > float32(defaultSlotLagMBAlertThreshold) {
					a.alertToSlack(ctx, slackAlertSender, alertKey,
						fmt.Sprintf(alertMessageTemplate, defaultSlotLagMBAlertThreshold))
				}
			}
		}
	}
}

func (a *Alerter) AlertIfOpenConnections(ctx context.Context, peerName string,
	openConnections *protos.GetOpenConnectionsForUserResult,
) {
	slackAlertSenders, err := a.registerSendersFromPool(ctx)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to set Slack senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", peerdbenv.PeerDBDeploymentUID())
	}

	// same as with slot lag, use lowest threshold for catalog
	defaultOpenConnectionsThreshold := dynamicconf.PeerDBOpenConnectionsAlertThreshold(ctx)
	lowestOpenConnectionsThreshold := defaultOpenConnectionsThreshold
	for _, slackAlertSender := range slackAlertSenders {
		if slackAlertSender.openConnectionsAlertThreshold > 0 {
			lowestOpenConnectionsThreshold = min(lowestOpenConnectionsThreshold, slackAlertSender.openConnectionsAlertThreshold)
		}
	}

	alertKey := peerName + "-max-open-connections-threshold-exceeded"
	alertMessageTemplate := fmt.Sprintf("%sOpen connections from PeerDB user `%s` on peer `%s`"+
		` has exceeded threshold size of %%d connections, currently at %d connections!
		cc: <!channel>`, deploymentUIDPrefix, openConnections.UserName, peerName, openConnections.CurrentOpenConnections)

	if openConnections.CurrentOpenConnections > int64(lowestOpenConnectionsThreshold) &&
		a.checkAndAddAlertToCatalog(ctx, alertKey, fmt.Sprintf(alertMessageTemplate, lowestOpenConnectionsThreshold)) {
		for _, slackAlertSender := range slackAlertSenders {
			if slackAlertSender.openConnectionsAlertThreshold > 0 {
				if openConnections.CurrentOpenConnections > int64(slackAlertSender.openConnectionsAlertThreshold) {
					a.alertToSlack(ctx, slackAlertSender, alertKey,
						fmt.Sprintf(alertMessageTemplate, slackAlertSender.openConnectionsAlertThreshold))
				}
			} else {
				if openConnections.CurrentOpenConnections > int64(defaultOpenConnectionsThreshold) {
					a.alertToSlack(ctx, slackAlertSender, alertKey,
						fmt.Sprintf(alertMessageTemplate, defaultOpenConnectionsThreshold))
				}
			}
		}
	}
}

func (a *Alerter) alertToSlack(ctx context.Context, slackAlertSender *slackAlertSender, alertKey string, alertMessage string) {
	err := slackAlertSender.sendAlert(ctx,
		":rotating_light:Alert:rotating_light:: "+alertKey, alertMessage)
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
		return
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertKey string, alertMessage string) bool {
	dur := dynamicconf.PeerDBAlertingGapMinutesAsDuration(ctx)
	if dur == 0 {
		logger.LoggerFromCtx(ctx).Warn("Alerting disabled via environment variable, returning")
		return false
	}

	row := a.catalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey)
	var createdTimestamp time.Time
	err := row.Scan(&createdTimestamp)
	if err != nil && err != pgx.ErrNoRows {
		logger.LoggerFromCtx(ctx).Warn("failed to send alert: ", slog.String("err", err.Error()))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		_, err = a.catalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message) VALUES($1,$2)",
			alertKey, alertMessage)
		if err != nil {
			logger.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}
	return false
}

func (a *Alerter) sendTelemetryMessage(ctx context.Context, flowName string, more string, level telemetry.Level) {
	if a.telemetrySender != nil {
		details := fmt.Sprintf("[%s] %s", flowName, more)
		_, err := a.telemetrySender.SendMessage(ctx, details, fmt.Sprintf("%s\n%+v", details, activity.GetInfo(ctx)), telemetry.Attributes{
			Level:         level,
			DeploymentUID: peerdbenv.PeerDBDeploymentUID(),
			Tags:          []string{flowName},
			Type:          flowName,
		})
		if err != nil {
			logger.LoggerFromCtx(ctx).Warn("failed to send message to telemetrySender", slog.Any("error", err))
			return
		}
	}
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) {
	errorWithStack := fmt.Sprintf("%+v", err)
	_, err = a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, errorWithStack, "error")
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to insert flow error", slog.Any("error", err))
		return
	}
	a.sendTelemetryMessage(ctx, flowName, errorWithStack, telemetry.ERROR)
}

func (a *Alerter) LogFlowEvent(ctx context.Context, flowName string, info string) {
	a.sendTelemetryMessage(ctx, flowName, info, telemetry.INFO)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, info string) {
	_, err := a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, info, "info")
	if err != nil {
		logger.LoggerFromCtx(ctx).Warn("failed to insert flow info", slog.Any("error", err))
		return
	}
}
