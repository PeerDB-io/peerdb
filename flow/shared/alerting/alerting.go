package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/dynamicconf"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

// alerting service, no cool name :(
type Alerter struct {
	catalogPool *pgxpool.Pool
	logger      *slog.Logger
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
func NewAlerter(catalogPool *pgxpool.Pool) (*Alerter, error) {
	logger := slog.Default()
	if catalogPool == nil {
		return nil, errors.New("catalog pool is nil for Alerter")
	}

	return &Alerter{
		catalogPool: catalogPool,
		logger:      logger,
	}, nil
}

func (a *Alerter) AlertIfSlotLag(ctx context.Context, peerName string, slotInfo *protos.SlotInfo) {
	slackAlertSenders, err := a.registerSendersFromPool(ctx)
	if err != nil {
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
				slog.Info("should alert slot lag")
				if slotInfo.LagInMb > float32(slackAlertSender.slotLagMBAlertThreshold) {
					slog.Info("Inside slack alert for slot")
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
				slog.Info("should alert connections")
				if openConnections.CurrentOpenConnections > int64(slackAlertSender.openConnectionsAlertThreshold) {
					slog.Info("Inside slack alert for conn")
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
		return
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertKey string, alertMessage string) bool {
	dur := dynamicconf.PeerDBAlertingGapMinutesAsDuration(ctx)
	if dur == 0 {
		return false
	}

	row := a.catalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey)
	var createdTimestamp time.Time
	err := row.Scan(&createdTimestamp)
	if err != nil && err != pgx.ErrNoRows {
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		a.AddAlertToCatalog(ctx, alertKey, alertMessage)
		a.AlertToSlack(ctx, alertKey, alertMessage)
	}
	return true
}

func (a *Alerter) AlertToSlack(ctx context.Context, alertKey string, alertMessage string) {
	slackAlertSenders, err := a.registerSendersFromPool(ctx)
	if err != nil {
		a.logger.WarnContext(ctx, "failed to set Slack senders", slog.Any("error", err))
		return
	}

	for _, slackAlertSender := range slackAlertSenders {
		err = slackAlertSender.sendAlert(ctx,
			fmt.Sprintf(":rotating_light:Alert:rotating_light:: %s", alertKey), alertMessage)
		if err != nil {
			a.logger.WarnContext(ctx, "failed to send alert", slog.Any("error", err))
			return
		}
	}
}

func (a *Alerter) AddAlertToCatalog(ctx context.Context, alertKey string, alertMessage string) {
	_, err := a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message) VALUES($1,$2)",
		alertKey, alertMessage)
	if err != nil {
		a.logger.WarnContext(ctx, "failed to insert alert", slog.Any("error", err))
		return
	}
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) {
	errorWithStack := fmt.Sprintf("%+v", err)
	_, err = a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, errorWithStack, "error")
	if err != nil {
		a.logger.WarnContext(ctx, "failed to insert flow error", slog.Any("error", err))
		return
	}
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, info string) {
	_, err := a.catalogPool.Exec(ctx,
		"INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, info, "info")
	if err != nil {
		a.logger.WarnContext(ctx, "failed to insert flow info", slog.Any("error", err))
		return
	}
}
