package e2e_s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

const (
	peerName string = "test_s3_peer"
)

type S3TestHelper struct {
	client     *s3.Client
	s3Config   *protos.S3Config
	bucketName string
	prefix     string
}

func NewS3TestHelper(switchToGCS bool) (*S3TestHelper, error) {
	credsPath := os.Getenv("TEST_S3_CREDS")
	bucketName := "peerdb-test-bucket"
	if switchToGCS {
		credsPath = os.Getenv("TEST_GCS_CREDS")
		bucketName = "peerdb_staging"
	}

	content, err := e2eshared.ReadFileToBytes(credsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config utils.S3PeerCredentials
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}
	endpoint := ""
	if switchToGCS {
		endpoint = "https://storage.googleapis.com"
	}
	client, err := utils.CreateS3Client(config)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("peerdb_test/%d_%s", time.Now().Unix(), shared.RandomString(6))
	return &S3TestHelper{
		client,
		&protos.S3Config{
			Url:             fmt.Sprintf("s3://%s/%s", bucketName, prefix),
			AccessKeyId:     &config.AccessKeyID,
			SecretAccessKey: &config.SecretAccessKey,
			Region:          &config.Region,
			Endpoint:        &endpoint,
			MetadataDb: &protos.PostgresConfig{
				Host:     "localhost",
				Port:     7132,
				Password: "postgres",
				User:     "postgres",
				Database: "postgres",
			},
		},
		bucketName,
		prefix,
	}, nil
}

func (h *S3TestHelper) GetPeer() *protos.Peer {
	return &protos.Peer{
		Name: peerName,
		Type: protos.DBType_S3,
		Config: &protos.Peer_S3Config{
			S3Config: h.s3Config,
		},
	}
}

// List all files from the S3 bucket.
// returns as a list of S3Objects.
func (h *S3TestHelper) ListAllFiles(
	ctx context.Context,
	jobName string,
) ([]s3types.Object, error) {
	Bucket := h.bucketName
	Prefix := fmt.Sprintf("%s/%s/", h.prefix, jobName)
	files, err := h.client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &Bucket,
		Prefix: &Prefix,
	})
	if err != nil {
		return nil, err
	}
	return files.Contents, nil
}

// Delete all generated objects during the test
func (h *S3TestHelper) CleanUp(ctx context.Context) error {
	Bucket := h.bucketName
	Prefix := h.prefix
	files, err := h.client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &Bucket,
		Prefix: &Prefix,
	})
	if err != nil {
		return err
	}

	// Delete each object
	for _, obj := range files.Contents {
		deleteInput := &s3.DeleteObjectInput{
			Bucket: &Bucket,
			Key:    obj.Key,
		}

		_, err := h.client.DeleteObject(ctx, deleteInput)
		if err != nil {
			return err
		}
	}

	return nil
}
