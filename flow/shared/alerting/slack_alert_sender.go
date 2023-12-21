package alerting

import (
	"context"
	"fmt"

	"github.com/slack-go/slack"
)

type slackAlertSender struct {
	client     *slack.Client
	channelIDs []string
}

type slackAlertConfig struct {
	AuthToken  string   `json:"auth_token"`
	ChannelIDs []string `json:"channel_ids"`
}

func newSlackAlertSender(config *slackAlertConfig) *slackAlertSender {
	return &slackAlertSender{
		client:     slack.New(config.AuthToken),
		channelIDs: config.ChannelIDs,
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
