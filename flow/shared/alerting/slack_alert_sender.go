package alerting

import (
	"context"
	"fmt"

	"github.com/slack-go/slack"
)

type slackAlertSender struct {
	client                        *slack.Client
	channelIDs                    []string
	slotLagMBAlertThreshold       uint32
	openConnectionsAlertThreshold uint32
}

type slackAlertConfig struct {
	AuthToken                     string   `json:"auth_token"`
	ChannelIDs                    []string `json:"channel_ids"`
	SlotLagMBAlertThreshold       uint32   `json:"slot_lag_mb_alert_threshold"`
	OpenConnectionsAlertThreshold uint32   `json:"open_connections_alert_threshold"`
}

func newSlackAlertSender(config *slackAlertConfig) *slackAlertSender {
	return &slackAlertSender{
		client:                        slack.New(config.AuthToken),
		channelIDs:                    config.ChannelIDs,
		slotLagMBAlertThreshold:       config.SlotLagMBAlertThreshold,
		openConnectionsAlertThreshold: config.OpenConnectionsAlertThreshold,
	}
}

func (s *slackAlertSender) sendAlert(ctx context.Context, alertTitle string, alertMessage string) error {
	for _, channelID := range s.channelIDs {
		_, _, _, err := s.client.SendMessageContext(ctx, channelID, slack.MsgOptionBlocks(
			slack.NewHeaderBlock(slack.NewTextBlockObject("plain_text", alertTitle, true, false)),
			slack.NewSectionBlock(slack.NewTextBlockObject("mrkdwn", alertMessage, false, false), nil, nil),
		))
		if err != nil {
			return fmt.Errorf("failed to send message to Slack channel %s: %w", channelID, err)
		}
	}
	return nil
}
