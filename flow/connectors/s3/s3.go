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
}

func NewS3Connector(ctx context.Context,
	s3ProtoConfig *protos.S3Config) (*S3Connector, error) {
	s3Client, err := utils.CreateS3Client()
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	return &S3Connector{
		ctx:    ctx,
		url:    s3ProtoConfig.Url,
		client: *s3Client,
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
