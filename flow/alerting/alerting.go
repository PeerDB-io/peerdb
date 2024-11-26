package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/telemetry"
	"github.com/PeerDB-io/peer-flow/tags"
)

// alerting service, no cool name :(
type Alerter struct {
	CatalogPool               *pgxpool.Pool
	snsTelemetrySender        telemetry.Sender
	incidentIoTelemetrySender telemetry.Sender
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
	rows, err := a.CatalogPool.Query(ctx,
		`SELECT id, service_type, service_config, enc_key_id, alert_for_mirrors
		FROM peerdb_stats.alerting_config`)
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	keys := peerdbenv.PeerDBEncKeys(ctx)
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
		shared.LoggerFromCtx(ctx).Info("Successfully registered sns telemetry sender")
		if err != nil {
			panic(fmt.Sprintf("unable to setup telemetry is nil for Alerter %+v", err))
		}
	}

	incidentIoURL := peerdbenv.PeerDBGetIncidentIoUrl()
	incidentIoAuth := peerdbenv.PeerDBGetIncidentIoToken()
	var incidentIoTelemetrySender telemetry.Sender
	if incidentIoURL != "" && incidentIoAuth != "" {
		var err error
		incidentIoTelemetrySender, err = telemetry.NewIncidentIoMessageSender(ctx, telemetry.IncidentIoMessageSenderConfig{
			URL:   incidentIoURL,
			Token: incidentIoAuth,
		})
		shared.LoggerFromCtx(ctx).Info("Successfully registered incident.io telemetry sender")
		if err != nil {
			panic(fmt.Sprintf("unable to setup incident.io telemetry is nil for Alerter %+v", err))
		}
	}

	return &Alerter{
		CatalogPool:               catalogPool,
		snsTelemetrySender:        snsMessageSender,
		incidentIoTelemetrySender: incidentIoTelemetrySender,
	}
}

func (a *Alerter) AlertIfSlotLag(ctx context.Context, alertKeys *AlertKeys, slotInfo *protos.SlotInfo) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", peerdbenv.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold, err := peerdbenv.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
		return
	}

	// catalog cannot use default threshold to space alerts properly, use the lowest set threshold instead
	lowestSlotLagMBAlertThreshold := defaultSlotLagMBAlertThreshold
	var alertSendersForMirrors []AlertSenderConfig
	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) == 0 || slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
			alertSendersForMirrors = append(alertSendersForMirrors, alertSenderConfig)
			if alertSenderConfig.Sender.getSlotLagMBAlertThreshold() > 0 {
				lowestSlotLagMBAlertThreshold = min(lowestSlotLagMBAlertThreshold, alertSenderConfig.Sender.getSlotLagMBAlertThreshold())
			}
		}
	}

	thresholdAlertKey := fmt.Sprintf("%s Slot Lag Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	thresholdAlertMessageTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!`, deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.LagInMb)

	badWalStatusAlertKey := fmt.Sprintf("%s Bad WAL Status for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	badWalStatusAlertMessage := fmt.Sprintf("%sSlot `%s` on peer `%s` has bad WAL status: `%s`",
		deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.WalStatus)

	for _, alertSenderConfig := range alertSendersForMirrors {
		if a.checkAndAddAlertToCatalog(ctx,
			alertSenderConfig.Id, thresholdAlertKey,
			fmt.Sprintf(thresholdAlertMessageTemplate, lowestSlotLagMBAlertThreshold)) {
			if alertSenderConfig.Sender.getSlotLagMBAlertThreshold() > 0 {
				if slotInfo.LagInMb > float32(alertSenderConfig.Sender.getSlotLagMBAlertThreshold()) {
					a.alertToProvider(ctx, alertSenderConfig, thresholdAlertKey,
						fmt.Sprintf(thresholdAlertMessageTemplate, alertSenderConfig.Sender.getSlotLagMBAlertThreshold()))
				}
			} else {
				if slotInfo.LagInMb > float32(defaultSlotLagMBAlertThreshold) {
					a.alertToProvider(ctx, alertSenderConfig, thresholdAlertKey,
						fmt.Sprintf(thresholdAlertMessageTemplate, defaultSlotLagMBAlertThreshold))
				}
			}
		}

		if (slotInfo.WalStatus == "lost" || slotInfo.WalStatus == "unreserved") &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, badWalStatusAlertKey, badWalStatusAlertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, badWalStatusAlertKey, badWalStatusAlertMessage)
		}
	}
}

func (a *Alerter) AlertIfOpenConnections(ctx context.Context, alertKeys *AlertKeys,
	openConnections *protos.GetOpenConnectionsForUserResult,
) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", peerdbenv.PeerDBDeploymentUID())
	}

	// same as with slot lag, use lowest threshold for catalog
	defaultOpenConnectionsThreshold, err := peerdbenv.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
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

func (a *Alerter) AlertIfTooLongSinceLastNormalize(ctx context.Context, alertKeys *AlertKeys, intervalSinceLastNormalize time.Duration) {
	intervalSinceLastNormalizeThreshold, err := peerdbenv.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	if err != nil {
		shared.LoggerFromCtx(ctx).
			Warn("failed to get interval since last normalize threshold from catalog", slog.Any("error", err))
	}

	if intervalSinceLastNormalizeThreshold == 0 {
		shared.LoggerFromCtx(ctx).Info("Alerting disabled via environment variable, returning")
		return
	}
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", peerdbenv.PeerDBDeploymentUID())
	}

	if intervalSinceLastNormalize > time.Duration(intervalSinceLastNormalizeThreshold)*time.Minute {
		alertKey := fmt.Sprintf("%s Too long since last data normalize for PeerDB mirror %s",
			deploymentUIDPrefix, alertKeys.FlowName)
		alertMessage := fmt.Sprintf("%sData hasn't been synced to the target for mirror `%s` since the last `%s`."+
			` This could indicate an issue with the pipeline — please check the UI and logs to confirm.`+
			` Alternatively, it might be that the source database is idle and not receiving new updates.`, deploymentUIDPrefix,
			alertKeys.FlowName, intervalSinceLastNormalize)

		for _, alertSenderConfig := range alertSenderConfigs {
			if len(alertSenderConfig.AlertForMirrors) == 0 ||
				slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
				if a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, alertKey, alertMessage) {
					a.alertToProvider(ctx, alertSenderConfig, alertKey, alertMessage)
				}
			}
		}
	}
}

func (a *Alerter) alertToProvider(ctx context.Context, alertSenderConfig AlertSenderConfig, alertKey string, alertMessage string) {
	if err := alertSenderConfig.Sender.sendAlert(ctx, alertKey, alertMessage); err != nil {
		shared.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertConfigId int64, alertKey string, alertMessage string) bool {
	logger := shared.LoggerFromCtx(ctx)
	dur, err := peerdbenv.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if err != nil {
		logger.Warn("failed to get alerting gap duration from catalog", slog.Any("error", err))
		return false
	}
	if dur == 0 {
		logger.Warn("Alerting disabled via environment variable, returning")
		return false
	}

	row := a.CatalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1 AND alert_config_id=$2
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey, alertConfigId)
	var createdTimestamp time.Time
	if err := row.Scan(&createdTimestamp); err != nil && err != pgx.ErrNoRows {
		shared.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("err", err))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		if _, err := a.CatalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message,alert_config_id) VALUES($1,$2,$3)",
			alertKey, alertMessage, alertConfigId,
		); err != nil {
			shared.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}

	logger.Info(fmt.Sprintf("Skipped sending alerts: last alert was sent at %s, which was <=%s ago", createdTimestamp.String(), dur.String()))
	return false
}

func (a *Alerter) sendTelemetryMessage(
	ctx context.Context,
	logger log.Logger,
	flowName string,
	more string,
	level telemetry.Level,
	additionalTags ...string,
) {
	allTags := []string{flowName, peerdbenv.PeerDBDeploymentUID()}
	allTags = append(allTags, additionalTags...)

	if flowTags, err := tags.GetTags(ctx, a.CatalogPool, flowName); err != nil {
		logger.Warn("failed to get flow tags", slog.Any("error", err))
	} else {
		for key, value := range flowTags {
			allTags = append(allTags, fmt.Sprintf("%s:%s", key, value))
		}
	}

	details := fmt.Sprintf("[%s] %s", flowName, more)
	attributes := telemetry.Attributes{
		Level:         level,
		DeploymentUID: peerdbenv.PeerDBDeploymentUID(),
		Tags:          allTags,
		Type:          flowName,
	}

	if a.snsTelemetrySender != nil {
		if response, err := a.snsTelemetrySender.SendMessage(ctx, details, details, attributes); err != nil {
			logger.Warn("failed to send message to snsTelemetrySender", slog.Any("error", err))
		} else {
			logger.Info("received response from snsTelemetrySender", slog.String("response", response))
		}
	}

	if a.incidentIoTelemetrySender != nil {
		if status, err := a.incidentIoTelemetrySender.SendMessage(ctx, details, details, attributes); err != nil {
			logger.Warn("failed to send message to incidentIoTelemetrySender", slog.Any("error", err))
		} else {
			logger.Info("received response from incident.io", slog.String("response", status))
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
	logger := shared.LoggerFromCtx(ctx)
	a.sendTelemetryMessage(ctx, logger, string(eventType)+":"+key, message, level)
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) {
	errorWithStack := fmt.Sprintf("%+v", err)
	logger := shared.LoggerFromCtx(ctx)
	logger.Error(err.Error(), slog.Any("stack", errorWithStack))
	if _, err := a.CatalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, errorWithStack, "error",
	); err != nil {
		logger.Warn("failed to insert flow error", slog.Any("error", err))
		return
	}
	var tags []string
	if errors.Is(err, context.Canceled) {
		tags = append(tags, "err:Canceled")
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		tags = append(tags, "err:EOF")
	}
	if errors.Is(err, net.ErrClosed) {
		tags = append(tags, "err:Closed")
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		tags = append(tags, "pgcode:"+pgErr.Code)
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		tags = append(tags, "err:Net")
	}
	a.sendTelemetryMessage(ctx, logger, flowName, errorWithStack, telemetry.ERROR, tags...)
}

func (a *Alerter) LogFlowEvent(ctx context.Context, flowName string, info string) {
	logger := shared.LoggerFromCtx(ctx)
	logger.Info(info)
	a.sendTelemetryMessage(ctx, logger, flowName, info, telemetry.INFO)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, info string) {
	logger := shared.LoggerFromCtx(ctx)
	logger.Info(info)
	if _, err := a.CatalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)", flowName, info, "info",
	); err != nil {
		logger.Warn("failed to insert flow info", slog.Any("error", err))
	}
}
