package evervigil

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikoksr/notify"
	"github.com/nikoksr/notify/service/slack"
	"github.com/sirupsen/logrus"
)

// alerting service, cool name
type EverVigil struct {
	notifier    *notify.Notify
	catalogPool *pgxpool.Pool
}

type slackServiceConfig struct {
	AuthToken  string   `json:"auth_token"`
	ChannelIDs []string `json:"channel_ids"`
}

func NewVigil(catalogPool *pgxpool.Pool) (*EverVigil, error) {
	notifier := notify.New()

	rows, err := catalogPool.Query(context.Background(),
		"SELECT service_type,service_config FROM peerdb_stats.alerting_config")
	if err != nil {
		return nil, fmt.Errorf("failed to read everVigil config from catalog: %w", err)
	}

	var serviceType, serviceConfig string
	_, err = pgx.ForEachRow(rows, []any{&serviceType, &serviceConfig}, func() error {
		switch serviceType {
		case "slack":
			var slackServiceConfig slackServiceConfig
			err = json.Unmarshal([]byte(serviceConfig), &slackServiceConfig)
			if err != nil {
				return fmt.Errorf("failed to unmarshal Slack service config: %w", err)
			}

			slackService := slack.New(slackServiceConfig.AuthToken)
			slackService.AddReceivers(slackServiceConfig.ChannelIDs...)
			notifier.UseServices(slackService)
		default:
			return fmt.Errorf("unknown service type: %s", serviceType)
		}
		return nil
	})

	return &EverVigil{
		notifier:    notifier,
		catalogPool: catalogPool,
	}, nil
}

func (ev *EverVigil) Close() {
	if ev.catalogPool != nil {
		ev.catalogPool.Close()
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past 15 minutes
func (ev *EverVigil) AlertIf(alertKey string, alertMessage string) {
	if ev.catalogPool != nil && ev.notifier != nil {
		row := ev.catalogPool.QueryRow(context.Background(),
			`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1
		 ORDER BY created_timestamp DESC LIMIT 1`,
			alertKey)
		var createdTimestamp time.Time
		err := row.Scan(&createdTimestamp)
		if err != nil && err != pgx.ErrNoRows {
			logrus.Warnf("failed to send alert: %v", err)
			return
		}

		if time.Since(createdTimestamp) >= 15*time.Minute {
			err = ev.notifier.Send(context.Background(),
				fmt.Sprintf(":rotating_light: *Alert Alert* :rotating_light:: %s since %s", alertKey,
					time.Now().Format("2006-01-02 15:04:05.999999")), alertMessage)
			if err != nil {
				logrus.Warnf("failed to send alert: %v", err)
				return
			}
			_, _ = ev.catalogPool.Exec(context.Background(),
				"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message) VALUES($1,$2)",
				alertKey, alertMessage)
		}
	}
}
