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
	"go.temporal.io/sdk/log"
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
				return alertSenderConfig, errors.New("missing sourceEmail for Email alerting service")
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

	defaultThreshold, err := internal.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
		return
	}

	prefix := getDeploymentUIDPrefix()
	alertKey := fmt.Sprintf("%sSlot Lag Threshold Exceeded for Peer %s", prefix, alertKeys.PeerName)
	msgTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!`, prefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.LagInMb)

	for _, sender := range filterSendersForMirror(alertSenderConfigs, alertKeys.FlowName) {
		threshold := sender.Sender.getSlotLagMBAlertThreshold()
		if threshold == 0 {
			threshold = defaultThreshold
		}
		a.sendAlertIfExceeded(ctx, sender,
			slotInfo.LagInMb > float32(threshold),
			alertKey, fmt.Sprintf(msgTemplate, threshold))

		// Bad WAL status alert
		if slotInfo.WalStatus == "lost" || slotInfo.WalStatus == "unreserved" {
			badWalKey := fmt.Sprintf("%sBad WAL Status for Peer %s", prefix, alertKeys.PeerName)
			badWalMsg := fmt.Sprintf("%sSlot `%s` on peer `%s` has bad WAL status: `%s`",
				prefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.WalStatus)
			a.sendAlertIfExceeded(ctx, sender, true, badWalKey, badWalMsg)
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

	defaultThreshold, err := internal.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
		return
	}

	prefix := getDeploymentUIDPrefix()
	alertKey := fmt.Sprintf("%sMax Open Connections Threshold Exceeded for Peer %s", prefix, alertKeys.PeerName)
	msgTemplate := fmt.Sprintf("%sOpen connections from PeerDB user `%s` on peer `%s`"+
		` has exceeded threshold size of %%d connections, currently at %d connections!`,
		prefix, openConnections.UserName, alertKeys.PeerName, openConnections.CurrentOpenConnections)

	for _, sender := range filterSendersForMirror(alertSenderConfigs, alertKeys.FlowName) {
		threshold := sender.Sender.getOpenConnectionsAlertThreshold()
		if threshold == 0 {
			threshold = defaultThreshold
		}
		a.sendAlertIfExceeded(ctx, sender,
			openConnections.CurrentOpenConnections > int64(threshold),
			alertKey, fmt.Sprintf(msgTemplate, threshold))
	}
}

func (a *Alerter) AlertIfTooLongSinceLastNormalize(ctx context.Context, alertKeys *AlertKeys, intervalSinceLastNormalize time.Duration) {
	defaultThreshold, err := internal.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get interval since last normalize threshold from catalog", slog.Any("error", err))
		return
	}
	if defaultThreshold == 0 {
		internal.LoggerFromCtx(ctx).Info("Alerting disabled via environment variable, returning")
		return
	}

	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	prefix := getDeploymentUIDPrefix()
	alertKey := fmt.Sprintf("%sToo long since last data normalize for PeerDB mirror %s", prefix, alertKeys.FlowName)
	msgTemplate := fmt.Sprintf("%sData hasn't been synced to the target for mirror `%s` since the last `%s`."+
		` This could indicate an issue with the pipeline — please check the UI and logs to confirm.`+
		` Alternatively, it might be that the source database is idle and not receiving new updates.`+
		` (threshold: %%d minutes)`, prefix, alertKeys.FlowName, intervalSinceLastNormalize)

	for _, sender := range filterSendersForMirror(alertSenderConfigs, alertKeys.FlowName) {
		threshold := sender.Sender.getIntervalSinceLastNormalizeMinutesThreshold()
		if threshold == 0 {
			threshold = defaultThreshold
		}
		a.sendAlertIfExceeded(ctx, sender,
			intervalSinceLastNormalize > time.Duration(threshold)*time.Minute,
			alertKey, fmt.Sprintf(msgTemplate, threshold))
	}
}

func (a *Alerter) alertToProvider(ctx context.Context, alertSenderConfig AlertSenderConfig, alertKey string, alertMessage string) {
	if err := alertSenderConfig.Sender.sendAlert(ctx, alertKey, alertMessage); err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
	}
}

// getDeploymentUIDPrefix returns the deployment UID prefix for alert messages
func getDeploymentUIDPrefix() string {
	if uid := internal.PeerDBDeploymentUID(); uid != "" {
		return fmt.Sprintf("[%s] ", uid)
	}
	return ""
}

// filterSendersForMirror returns senders that apply to a specific mirror
func filterSendersForMirror(senders []AlertSenderConfig, flowName string) []AlertSenderConfig {
	var filtered []AlertSenderConfig
	for _, sender := range senders {
		if len(sender.AlertForMirrors) == 0 || slices.Contains(sender.AlertForMirrors, flowName) {
			filtered = append(filtered, sender)
		}
	}
	return filtered
}

// sendAlertIfExceeded handles the common pattern of checking threshold and sending alert
func (a *Alerter) sendAlertIfExceeded(
	ctx context.Context,
	sender AlertSenderConfig,
	exceeded bool,
	alertKey string,
	alertMessage string,
) {
	if exceeded {
		if a.checkAndAddAlertToCatalog(ctx, sender.Id, alertKey, alertMessage) {
			a.alertToProvider(ctx, sender, alertKey, alertMessage)
		}
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
	logger log.Logger,
	flowName string,
	more string,
	level telemetry.Level,
	additionalTags ...string,
) {
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
	logger := internal.LoggerFromCtx(ctx)
	a.sendTelemetryMessage(ctx, logger, string(eventType)+":"+key, message, level)
}

type flowErrorType string

func (f flowErrorType) String() string {
	return string(f)
}

const (
	flowErrorTypeWarn  flowErrorType = "warn"
	flowErrorTypeError flowErrorType = "error"
)

// logFlowErrorInternal pushes the error to the errors table and emits a metric as well as a telemetry message
func (a *Alerter) logFlowErrorInternal(
	ctx context.Context,
	flowName string,
	errorType flowErrorType,
	inErr error,
	loggerFunc func(string, ...any),
) {
	logger := internal.LoggerFromCtx(ctx)
	inErrWithStack := fmt.Sprintf("%+v", inErr)
	errError := inErr.Error()
	loggerFunc(errError, slog.String("stack", inErrWithStack))
	if _, err := a.CatalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, inErrWithStack, errorType.String(),
	); err != nil {
		logger.Error("failed to insert flow error", slog.Any("error", err))
		return
	}

	var tags []string
	if errors.Is(inErr, context.Canceled) {
		tags = append(tags, string(shared.ErrTypeCanceled))
	}
	if errors.Is(inErr, io.EOF) || errors.Is(inErr, io.ErrUnexpectedEOF) {
		tags = append(tags, string(shared.ErrTypeEOF))
	}
	if errors.Is(inErr, net.ErrClosed) || strings.HasSuffix(errError, "use of closed network connection") {
		tags = append(tags, string(shared.ErrTypeClosed))
	}
	var pgErr *pgconn.PgError
	if errors.As(inErr, &pgErr) {
		tags = append(tags, "pgcode:"+pgErr.Code)
	}
	var myErr *mysql.MyError
	if errors.As(inErr, &myErr) {
		tags = append(tags, fmt.Sprintf("mycode:%d", myErr.Code), "mystate:"+myErr.State)
	}
	var mongoErr *driver.Error
	if errors.As(inErr, &mongoErr) {
		tags = append(tags, fmt.Sprintf("mongocode:%d", mongoErr.Code))
	}
	var chErr *clickhouse.Exception
	if errors.As(inErr, &chErr) {
		tags = append(tags, fmt.Sprintf("chcode:%d", chErr.Code))
	}
	var netErr *net.OpError
	if errors.As(inErr, &netErr) {
		tags = append(tags, string(shared.ErrTypeNet))
	}
	// For SSH connection errors, we currently tag them as "err:Net"
	var sshErr *ssh.OpenChannelError
	if errors.As(inErr, &sshErr) {
		tags = append(tags, string(shared.ErrTypeNet))
	}

	errorClass, errInfo := GetErrorClass(ctx, inErr)
	tags = append(tags, "errorClass:"+errorClass.String(), "errorAction:"+errorClass.ErrorAction().String())

	// Only send alerts to telemetry sender (incident.io) if the env is enabled
	if internal.PeerDBTelemetrySenderSendErrorAlertsEnabled(ctx) {
		a.sendTelemetryMessage(ctx, logger, flowName, inErrWithStack, telemetry.ERROR, tags...)
	}
	loggerFunc(fmt.Sprintf("Emitting error/warning metric: '%s'", errError),
		slog.Any("error", inErr),
		slog.Any("errorClass", errorClass),
		slog.Any("errorInfo", errInfo),
		slog.Any("stack", inErrWithStack),
		slog.String("flowErrorType", errorType.String()),
	)
	errorAttributes := []attribute.KeyValue{
		attribute.Stringer(otel_metrics.ErrorClassKey, errorClass),
		attribute.Stringer(otel_metrics.ErrorActionKey, errorClass.ErrorAction()),
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
	if errorType == flowErrorTypeWarn {
		counter = a.otelManager.Metrics.WarningEmittedCounter
		gauge = a.otelManager.Metrics.WarningsEmittedGauge
	}
	counter.Add(ctx, 1, errorAttributeSet)
	gauge.Record(ctx, 1, errorAttributeSet)
}

func (a *Alerter) LogFlowWrappedError(ctx context.Context, flowName string, s string, inErr error) error {
	logger := internal.LoggerFromCtx(ctx)
	err := shared.WrapError(s, inErr)
	a.logFlowErrorInternal(ctx, flowName, flowErrorTypeError, err, logger.Error)
	return err
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, inErr error) error {
	logger := internal.LoggerFromCtx(ctx)
	a.logFlowErrorInternal(ctx, flowName, flowErrorTypeError, inErr, logger.Error)
	return inErr
}

func (a *Alerter) LogFlowWarning(ctx context.Context, flowName string, inErr error) {
	logger := internal.LoggerFromCtx(ctx)
	a.logFlowErrorInternal(ctx, flowName, flowErrorTypeWarn, inErr, logger.Warn)
}

func (a *Alerter) LogFlowEvent(ctx context.Context, flowName string, info string) {
	logger := internal.LoggerFromCtx(ctx)
	logger.Info(info)
	a.sendTelemetryMessage(ctx, logger, flowName, info, telemetry.INFO)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, info string) {
	logger := internal.LoggerFromCtx(ctx)
	logger.Info(info)
	if _, err := a.CatalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)", flowName, info, "info",
	); err != nil {
		logger.Warn("failed to insert flow info", slog.Any("error", err))
	}
}
