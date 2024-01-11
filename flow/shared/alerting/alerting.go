package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/dynamicconf"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// alerting service, no cool name :(
type Alerter struct {
	catalogPool *pgxpool.Pool
	logger      *slog.Logger
}

func registerSendersFromPool(ctx context.Context, catalogPool *pgxpool.Pool) ([]*slackAlertSender, error) {
	rows, err := catalogPool.Query(ctx,
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
		logger.Error("catalog pool is nil for Alerter")
		return nil, fmt.Errorf("catalog pool is nil for Alerter")
	}

	return &Alerter{
		catalogPool: catalogPool,
		logger:      logger,
	}, nil
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
func (a *Alerter) AlertIf(ctx context.Context, alertKey string, alertMessage string) {
	dur := dynamicconf.PeerDBAlertingGapMinutesAsDuration(ctx)
	if dur == 0 {
		a.logger.WarnContext(ctx, "Alerting disabled via environment variable, returning")
		return
	}

	var err error
	row := a.catalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey)
	var createdTimestamp time.Time
	err = row.Scan(&createdTimestamp)
	if err != nil && err != pgx.ErrNoRows {
		a.logger.Warn("failed to send alert: ", slog.String("err", err.Error()))
		return
	}

	if time.Since(createdTimestamp) >= dur {
		a.AddAlertToCatalog(ctx, alertKey, alertMessage)
		a.AlertToSlack(ctx, alertKey, alertMessage)
	}
}

func (a *Alerter) AlertToSlack(ctx context.Context, alertKey string, alertMessage string) {
	slackAlertSenders, err := registerSendersFromPool(ctx, a.catalogPool)
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
