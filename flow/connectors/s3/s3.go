package conns3

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

type S3Connector struct {
	ctx    context.Context
	url    string
	client s3.S3
	creds  utils.S3PeerCredentials
}

func NewS3Connector(ctx context.Context,
	s3ProtoConfig *protos.S3Config) (*S3Connector, error) {
	keyID := ""
	if s3ProtoConfig.AccessKeyId != nil {
		keyID = *s3ProtoConfig.AccessKeyId
	}
	secretKey := ""
	if s3ProtoConfig.SecretAccessKey != nil {
		secretKey = *s3ProtoConfig.SecretAccessKey
	}
	roleArn := ""
	if s3ProtoConfig.RoleArn != nil {
		roleArn = *s3ProtoConfig.RoleArn
	}
	region := ""
	if s3ProtoConfig.Region != nil {
		region = *s3ProtoConfig.Region
	}
	endpoint := ""
	if s3ProtoConfig.Endpoint != nil {
		endpoint = *s3ProtoConfig.Endpoint
	}
	s3PeerCreds := utils.S3PeerCredentials{
		AccessKeyID:     keyID,
		SecretAccessKey: secretKey,
		AwsRoleArn:      roleArn,
		Region:          region,
		Endpoint:        endpoint,
	}
	s3Client, err := utils.CreateS3Client(s3PeerCreds)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	return &S3Connector{
		ctx:    ctx,
		url:    s3ProtoConfig.Url,
		client: *s3Client,
		creds:  s3PeerCreds,
	}, nil
}

func (c *S3Connector) Close() error {
	log.Debugf("Closing s3 connector is a noop")
	return nil
}

func (c *S3Connector) ConnectionActive() bool {
	_, err := c.client.ListBuckets(nil)
	return err == nil
}
