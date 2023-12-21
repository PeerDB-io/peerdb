package alerting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// alerting service, no cool name :(
type Alerter struct {
	catalogPool *pgxpool.Pool
	logger      *slog.Logger
}

func registerSendersFromPool(catalogPool *pgxpool.Pool) ([]*slackAlertSender, error) {
	rows, err := catalogPool.Query(context.Background(),
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
func NewAlerter(catalogPool *pgxpool.Pool) *Alerter {
	return &Alerter{
		catalogPool: catalogPool,
		logger:      slog.Default(),
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
func (a *Alerter) AlertIf(ctx context.Context, alertKey string, alertMessage string) {
	if peerdbenv.GetPeerDBAlertingGapMinutesAsDuration() == 0 {
		a.logger.WarnContext(ctx, "Alerting disabled via environment variable, returning")
		return
	}

	if a.catalogPool != nil {
		slackAlertSenders, err := registerSendersFromPool(a.catalogPool)
		if err != nil {
			a.logger.WarnContext(ctx, "failed to set Slack senders", slog.Any("error", err))
			return
		}
		if len(slackAlertSenders) == 0 {
			a.logger.WarnContext(ctx, "no Slack senders configured, returning")
			return
		}

		row := a.catalogPool.QueryRow(context.Background(),
			`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1
		 ORDER BY created_timestamp DESC LIMIT 1`,
			alertKey)
		var createdTimestamp time.Time
		err = row.Scan(&createdTimestamp)
		if err != nil && err != pgx.ErrNoRows {
			a.logger.Warn("failed to send alert: %v", err)
			return
		}

		if time.Since(createdTimestamp) >= peerdbenv.GetPeerDBAlertingGapMinutesAsDuration() {
			for _, slackAlertSender := range slackAlertSenders {
				err = slackAlertSender.sendAlert(context.Background(),
					fmt.Sprintf(":rotating_light:Alert:rotating_light:: %s", alertKey), alertMessage)
				if err != nil {
					a.logger.WarnContext(ctx, "failed to send alert", slog.Any("error", err))
					return
				}
				_, _ = a.catalogPool.Exec(context.Background(),
					"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message) VALUES($1,$2)",
					alertKey, alertMessage)
			}
		}
	}
}
