package telemetry

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"strings"
)

type SNSMessageSender interface {
	Sender
}

type SNSMessageSenderImpl struct {
	client *sns.Client
	topic  string
}

type SNSMessageSenderConfig struct {
	Topic string `json:"topic"`
}

func (S *SNSMessageSenderImpl) SendMessage(ctx context.Context, subject string, body string, attributes Attributes) (*string, error) {
	publish, err := S.client.Publish(ctx, &sns.PublishInput{
		Message: aws.String(body),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"level": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(attributes.Level)),
			},
			"tags": {
				DataType:    aws.String("String"),
				StringValue: aws.String(strings.Join(attributes.Tags, ",")),
			},
			"deploymentUUID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(attributes.DeploymentUID)),
			},
			"entity": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(attributes.DeploymentUID)),
			},
			"type": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(attributes.Type)),
			},
		},
		Subject:  aws.String(subject),
		TopicArn: aws.String(S.topic),
	})
	if err != nil {
		return nil, err
	}
	return publish.MessageId, nil
}

func NewSNSMessageSenderWithNewClient(ctx context.Context, config *SNSMessageSenderConfig) (SNSMessageSender, error) {
	client, err := newSnsClient(ctx)
	if err != nil {
		return nil, err
	}
	return &SNSMessageSenderImpl{
		client: client,
		topic:  config.Topic,
	}, nil
}

func NewSNSMessageSender(client *sns.Client, config *SNSMessageSenderConfig) SNSMessageSender {
	return &SNSMessageSenderImpl{
		client: client,
		topic:  config.Topic,
	}
}

func newSnsClient(ctx context.Context) (*sns.Client, error) {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	snsClient := sns.NewFromConfig(sdkConfig)
	return snsClient, nil
}
