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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/crypto/ssh"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

// alerting service, no cool name :(
type Alerter struct {
	shared.CatalogPool
	snsTelemetrySender        telemetry.Sender
	incidentIoTelemetrySender telemetry.Sender
	otelManager               *otel_metrics.OtelManager
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

// doesn't take care of closing pool, needs to be done externally.
func NewAlerter(ctx context.Context, catalogPool shared.CatalogPool, otelManager *otel_metrics.OtelManager) *Alerter {
	if catalogPool.Pool == nil {
		panic("catalog pool is nil for Alerter")
	}
	snsTopic := internal.PeerDBTelemetryAWSSNSTopicArn()
	var snsMessageSender telemetry.Sender
	if snsTopic != "" {
		var err error
		snsMessageSender, err = telemetry.NewSNSMessageSenderWithNewClient(ctx, &telemetry.SNSMessageSenderConfig{
			Topic: snsTopic,
		})
		internal.LoggerFromCtx(ctx).Info("Successfully registered sns telemetry sender")
		if err != nil {
			panic(fmt.Sprintf("unable to setup telemetry is nil for Alerter %+v", err))
		}
	}

	incidentIoURL := internal.PeerDBGetIncidentIoUrl()
	incidentIoAuth := internal.PeerDBGetIncidentIoToken()
	var incidentIoTelemetrySender telemetry.Sender
	if incidentIoURL != "" && incidentIoAuth != "" {
		var err error
		incidentIoTelemetrySender, err = telemetry.NewIncidentIoMessageSender(ctx, telemetry.IncidentIoMessageSenderConfig{
			URL:   incidentIoURL,
			Token: incidentIoAuth,
		})
		internal.LoggerFromCtx(ctx).Info("Successfully registered incident.io telemetry sender")
		if err != nil {
			panic(fmt.Sprintf("unable to setup incident.io telemetry is nil for Alerter %+v", err))
		}
	}

	return &Alerter{
		CatalogPool:               catalogPool,
		snsTelemetrySender:        snsMessageSender,
		incidentIoTelemetrySender: incidentIoTelemetrySender,
		otelManager:               otelManager,
	}
}

func (a *Alerter) registerSendersFromPool(ctx context.Context) ([]AlertSenderConfig, error) {
	rows, err := a.CatalogPool.Query(ctx,
		`SELECT id, service_type, service_config, enc_key_id, alert_for_mirrors
		FROM peerdb_stats.alerting_config`)
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	keys := internal.PeerDBEncKeys(ctx)
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
				internal.PeerDBAlertingEmailSenderReplyToAddresses()); replyToEnvString != "" {
				replyToAddresses = strings.Split(replyToEnvString, ",")
			}
			emailServiceConfig := EmailAlertSenderConfig{
				sourceEmail:          internal.PeerDBAlertingEmailSenderSourceEmail(),
				configurationSetName: internal.PeerDBAlertingEmailSenderConfigurationSet(),
				replyToAddresses:     replyToAddresses,
			}
			if emailServiceConfig.sourceEmail == "" {
				return alertSenderConfig, fmt.Errorf("missing sourceEmail for Email alerting service")
			}
			if err := json.Unmarshal(serviceConfig, &emailServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}
			var region *string
			if envRegion := internal.PeerDBAlertingEmailSenderRegion(); envRegion != "" {
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

func (a *Alerter) AlertIfSlotLag(ctx context.Context, alertKeys *AlertKeys, slotInfo *protos.SlotInfo) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", internal.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold, err := internal.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
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
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}

	// same as with slot lag, use lowest threshold for catalog
	defaultOpenConnectionsThreshold, err := internal.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
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
	intervalSinceLastNormalizeThreshold, err := internal.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).
			Warn("failed to get interval since last normalize threshold from catalog", slog.Any("error", err))
	}

	if intervalSinceLastNormalizeThreshold == 0 {
		internal.LoggerFromCtx(ctx).Info("Alerting disabled via environment variable, returning")
		return
	}
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
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
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertConfigId int64, alertKey string, alertMessage string) bool {
	logger := internal.LoggerFromCtx(ctx)
	dur, err := internal.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if err != nil {
		logger.Warn("failed to get alerting gap duration from catalog", slog.Any("error", err))
		return false
	}
	if dur == 0 {
		logger.Warn("Alerting disabled via environment variable, returning")
		return false
	}

	var createdTimestamp time.Time
	if err := a.CatalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1 AND alert_config_id=$2
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey, alertConfigId,
	).Scan(&createdTimestamp); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("err", err))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		if _, err := a.CatalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message,alert_config_id) VALUES($1,$2,$3)",
			alertKey, alertMessage, alertConfigId,
		); err != nil {
			internal.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}

	logger.Info(fmt.Sprintf("Skipped sending alerts: last alert was sent at %s, which was <=%s ago", createdTimestamp.String(), dur.String()))
	return false
}

func (a *Alerter) sendTelemetryMessage(
	ctx context.Context,
	flowName string,
	more string,
	level telemetry.Level,
	additionalTags ...string,
) {
	logger := internal.LoggerFromCtx(ctx)
	allTags := []string{flowName, internal.PeerDBDeploymentUID()}
	allTags = append(allTags, additionalTags...)

	if flowTags, err := GetTags(ctx, a.CatalogPool, flowName); err != nil {
		logger.Warn("failed to get flow tags", slog.Any("error", err))
	} else {
		for key, value := range flowTags {
			allTags = append(allTags, fmt.Sprintf("%s:%s", key, value))
		}
	}

	details := fmt.Sprintf("[%s] %s", flowName, more)
	attributes := telemetry.Attributes{
		Level:         level,
		DeploymentUID: internal.PeerDBDeploymentUID(),
		Tags:          allTags,
		Type:          flowName,
	}

	if a.snsTelemetrySender != nil {
		if response, err := a.snsTelemetrySender.SendMessage(ctx, details, details, attributes); err != nil {
			logger.Warn("failed to send message to snsTelemetrySender", slog.Any("error", err))
		} else {
			logger.Debug("received response from snsTelemetrySender", slog.String("response", response))
		}
	}

	if a.incidentIoTelemetrySender != nil {
		if status, err := a.incidentIoTelemetrySender.SendMessage(ctx, details, details, attributes); err != nil {
			logger.Warn("failed to send message to incidentIoTelemetrySender", slog.Any("error", err))
		} else {
			logger.Debug("received response from incident.io", slog.String("response", status))
		}
	}
}

func (a *Alerter) emitNonFlowTelemetryEvent(
	ctx context.Context, eventType telemetry.EventType, key string, message string, level telemetry.Level,
) {
	a.sendTelemetryMessage(ctx, string(eventType)+":"+key, message, level)
}

// Wrapper for different telemetry levels for non-flow events
func (a *Alerter) EmitNonFlowInfoTelemetryEvent(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.emitNonFlowTelemetryEvent(ctx, eventType, key, message, telemetry.INFO)
}

func (a *Alerter) EmitNonFlowWarningTelemetryEvent(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.emitNonFlowTelemetryEvent(ctx, eventType, key, message, telemetry.WARN)
}

func (a *Alerter) EmitNonFlowErrorTelemetryEvent(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.emitNonFlowTelemetryEvent(ctx, eventType, key, message, telemetry.ERROR)
}

func (a *Alerter) EmitNonFlowCriticalTelemetryEvent(ctx context.Context, eventType telemetry.EventType, key string, message string) {
	a.emitNonFlowTelemetryEvent(ctx, eventType, key, message, telemetry.CRITICAL)
}

func (a *Alerter) EmitFlowInfoTelemetryEvent(ctx context.Context, flowName string, info string) {
	logger := internal.LoggerFromCtx(ctx)
	logger.Info(info)
	a.sendTelemetryMessage(ctx, flowName, info, telemetry.INFO)
}

type FlowErrorType string

func (f FlowErrorType) String() string {
	return string(f)
}

const (
	FlowErrorTypeInfo  FlowErrorType = "info"
	FlowErrorTypeWarn  FlowErrorType = "warn"
	FlowErrorTypeError FlowErrorType = "error"
)

// recordFlowErrorInternal pushes the error to the errors table and emits a metric as well as a telemetry message.
func (a *Alerter) recordFlowErrorInternal(
	ctx context.Context,
	flowName string,
	errorType FlowErrorType,
	err error,
) {
	logger := internal.LoggerFromCtx(ctx)
	errClass, errInfo := GetErrorClass(ctx, err)
	errMessage := fmt.Sprintf("%+v", err)

	// 1. Log the error internally
	logFn := logger.Error
	if errorType == FlowErrorTypeWarn {
		logFn = logger.Warn
	}
	logFn(err.Error(),
		slog.Any("errorClass", errClass),
		slog.Any("errorInfo", errInfo),
		slog.Any("errorMessage", errMessage),
		slog.String("flowErrorType", errorType.String()),
	)

	if ctx.Err() != nil {
		logger.Error("Skipping flow error handling: " + ctx.Err().Error())
		return
	}

	// 2. Insert log to flow_errors table
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, errMessage, errorType); err != nil {
		logger.Error("failed to insert flow error", slog.Any("error", err))
	}

	// 3. Only send alerts to telemetry sender (incident.io) if the env is enabled
	if internal.PeerDBTelemetrySenderSendErrorAlertsEnabled(ctx) {
		var tags []string
		if errors.Is(err, context.Canceled) {
			tags = append(tags, string(shared.ErrTypeCanceled))
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			tags = append(tags, string(shared.ErrTypeEOF))
		}
		if errors.Is(err, net.ErrClosed) || strings.HasSuffix(err.Error(), "use of closed network connection") {
			tags = append(tags, string(shared.ErrTypeClosed))
		}
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			tags = append(tags, "pgcode:"+pgErr.Code)
		}
		if myErr, ok := errors.AsType[*mysql.MyError](err); ok {
			tags = append(tags, fmt.Sprintf("mycode:%d", myErr.Code), "mystate:"+myErr.State)
		}
		if mongoErr, ok := errors.AsType[*driver.Error](err); ok {
			tags = append(tags, fmt.Sprintf("mongocode:%d", mongoErr.Code))
		}
		if chErr, ok := errors.AsType[*clickhouse.Exception](err); ok {
			tags = append(tags, fmt.Sprintf("chcode:%d", chErr.Code))
		}
		if _, ok := errors.AsType[*net.OpError](err); ok {
			tags = append(tags, string(shared.ErrTypeNet))
		}
		// For SSH connection errors, we currently tag them as "err:Net"
		if _, ok := errors.AsType[*ssh.OpenChannelError](err); ok {
			tags = append(tags, string(shared.ErrTypeNet))
		}
		tags = append(tags, "errorClass:"+errClass.String(), "errorAction:"+errClass.ErrorAction().String())
		a.sendTelemetryMessage(ctx, flowName, errMessage, telemetry.ERROR, tags...)
	}

	// 4. Record error metrics
	errorAttributes := []attribute.KeyValue{
		attribute.Stringer(otel_metrics.ErrorClassKey, errClass),
		attribute.Stringer(otel_metrics.ErrorActionKey, errClass.ErrorAction()),
		attribute.Stringer(otel_metrics.ErrorSourceKey, errInfo.Source),
		attribute.String(otel_metrics.ErrorCodeKey, errInfo.Code),
	}
	if len(errInfo.AdditionalAttributes) != 0 {
		for k, v := range errInfo.AdditionalAttributes {
			errorAttributes = append(errorAttributes, attribute.String(k.String(), v))
		}
	}
	errorAttributeSet := metric.WithAttributeSet(attribute.NewSet(errorAttributes...))
	counter := a.otelManager.Metrics.ErrorsEmittedCounter
	gauge := a.otelManager.Metrics.ErrorEmittedGauge
	if errorType == FlowErrorTypeWarn {
		counter = a.otelManager.Metrics.WarningEmittedCounter
		gauge = a.otelManager.Metrics.WarningsEmittedGauge
	}
	counter.Add(ctx, 1, errorAttributeSet)
	gauge.Record(ctx, 1, errorAttributeSet)
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) error {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeError, err)
	return err
}

func (a *Alerter) LogFlowWarning(ctx context.Context, flowName string, err error) {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeWarn, err)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, message string) {
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, message, FlowErrorTypeInfo); err != nil {
		logger := internal.SlogLoggerFromCtx(ctx)
		logger.WarnContext(ctx, "failed to insert flow info", slog.Any("error", err))
	}
}

// InsertFlowLog Historically flow_errors table only stored errors, hence the name.
// It has since evolved to surface all user-facing logs.
func InsertFlowLog(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	flowName string,
	message string,
	errorType FlowErrorType,
) error {
	logger := internal.SlogLoggerFromCtx(ctx)
	logger.InfoContext(ctx, fmt.Sprintf("inserting user-facing flow log: [%s] %s", errorType.String(), message),
		slog.String("flowName", flowName),
		slog.String("severity", errorType.String()),
		slog.String("message", message),
		slog.String("logAudience", "user"),
	)
	_, err := catalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, message, errorType.String(),
	)
	return err
}
