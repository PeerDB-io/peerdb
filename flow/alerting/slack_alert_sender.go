package alerting

import (
	"context"
	"fmt"
	"strings"

	"github.com/slack-go/slack"
)

type SlackAlertSender struct {
	AlertSender
	client                        *slack.Client
	channelIDs                    []string
	members                       []string
	slotLagMBAlertThreshold       uint32
	openConnectionsAlertThreshold uint32
}

func (s *SlackAlertSender) getSlotLagMBAlertThreshold() uint32 {
	return s.slotLagMBAlertThreshold
}

func (s *SlackAlertSender) getOpenConnectionsAlertThreshold() uint32 {
	return s.openConnectionsAlertThreshold
}

type slackAlertConfig struct {
	AuthToken                     string   `json:"auth_token"`
	ChannelIDs                    []string `json:"channel_ids"`
	Members                       []string `json:"members"`
	SlotLagMBAlertThreshold       uint32   `json:"slot_lag_mb_alert_threshold"`
	OpenConnectionsAlertThreshold uint32   `json:"open_connections_alert_threshold"`
}

func newSlackAlertSender(config *slackAlertConfig) *SlackAlertSender {
	return &SlackAlertSender{
		client:                        slack.New(config.AuthToken),
		channelIDs:                    config.ChannelIDs,
		slotLagMBAlertThreshold:       config.SlotLagMBAlertThreshold,
		openConnectionsAlertThreshold: config.OpenConnectionsAlertThreshold,
		members:                       config.Members,
	}
}

func formatCCMembers(members []string) string {
	if len(members) == 0 {
		return "cc: <!channel>"
	}
	var b strings.Builder
	b.WriteString("cc:")
	for _, member := range members {
		b.WriteString(" <@")
		b.WriteString(member)
		b.WriteString(">")
	}
	return b.String()
}

func (s *SlackAlertSender) sendAlert(ctx context.Context, alertTitle string, alertMessage string) error {
	ccMembersPart := formatCCMembers(s.members)
	for _, channelID := range s.channelIDs {
		_, _, _, err := s.client.SendMessageContext(ctx, channelID, slack.MsgOptionBlocks(
			slack.NewHeaderBlock(slack.NewTextBlockObject("plain_text", ":rotating_light:Alert:rotating_light:: "+alertTitle, true, false)),
			slack.NewSectionBlock(slack.NewTextBlockObject("mrkdwn", alertMessage+"\n"+ccMembersPart, false, false), nil, nil),
		))
		if err != nil {
			return fmt.Errorf("failed to send message to Slack channel %s: %w", channelID, err)
		}
	}
	return nil
}
