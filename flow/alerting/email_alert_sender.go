package alerting

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared/aws_common"
)

type EmailAlertSender interface {
	AlertSender
}

type emailAlertSenderImpl struct {
	client                        *ses.Client
	sourceEmail                   string
	configurationSetName          string
	slotLagMBAlertThreshold       uint32
	openConnectionsAlertThreshold uint32
	emailAddresses                []string
}

func (e *emailAlertSenderImpl) getSlotLagMBAlertThreshold() uint32 {
	return e.slotLagMBAlertThreshold
}

func (e *emailAlertSenderImpl) getOpenConnectionsAlertThreshold() uint32 {
	return e.openConnectionsAlertThreshold
}

type EmailAlertSenderConfig struct {
	sourceEmail                   string
	configurationSetName          string
	SlotLagMBAlertThreshold       uint32   `json:"slot_lag_mb_alert_threshold"`
	OpenConnectionsAlertThreshold uint32   `json:"open_connections_alert_threshold"`
	EmailAddresses                []string `json:"email_addresses"`
}

func (e *emailAlertSenderImpl) sendAlert(ctx context.Context, alertTitle string, alertMessage string) error {
	_, err := e.client.SendEmail(ctx, &ses.SendEmailInput{
		Destination: &types.Destination{
			ToAddresses: e.emailAddresses,
		},
		Message: &types.Message{
			Body: &types.Body{
				Text: &types.Content{
					Data:    aws.String(alertMessage),
					Charset: aws.String("utf-8"),
				},
			},
			Subject: &types.Content{
				Data:    aws.String(alertTitle),
				Charset: aws.String("utf-8"),
			},
		},
		Source:               aws.String(e.sourceEmail),
		ConfigurationSetName: aws.String(e.configurationSetName),
		Tags: []types.MessageTag{
			{Name: aws.String("DeploymentUUID"), Value: aws.String(peerdbenv.PeerDBDeploymentUID())},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func NewEmailAlertSenderWithNewClient(ctx context.Context, region *string, config *EmailAlertSenderConfig) (EmailAlertSender, error) {
	client, err := newSesClient(ctx, region)
	if err != nil {
		return nil, err
	}
	return NewEmailAlertSender(client, config), nil
}

func NewEmailAlertSender(client *ses.Client, config *EmailAlertSenderConfig) EmailAlertSender {
	return &emailAlertSenderImpl{
		client:                        client,
		sourceEmail:                   config.sourceEmail,
		configurationSetName:          config.configurationSetName,
		slotLagMBAlertThreshold:       config.SlotLagMBAlertThreshold,
		openConnectionsAlertThreshold: config.OpenConnectionsAlertThreshold,
		emailAddresses:                config.EmailAddresses,
	}
}

func newSesClient(ctx context.Context, region *string) (*ses.Client, error) {
	sdkConfig, err := aws_common.LoadSdkConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	snsClient := ses.NewFromConfig(*sdkConfig)
	return snsClient, nil
}
