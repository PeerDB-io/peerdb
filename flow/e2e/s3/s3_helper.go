package e2e_s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const (
	peerName   string = "test_s3_peer"
	prefixName string = "test-s3"
)

type S3TestHelper struct {
	client     *s3.S3
	s3Config   *protos.S3Config
	bucketName string
}

func NewS3TestHelper(switchToGCS bool) (*S3TestHelper, error) {
	credsPath := os.Getenv("TEST_S3_CREDS")
	bucketName := "peerdb-test-bucket"
	if switchToGCS {
		credsPath = os.Getenv("TEST_GCS_CREDS")
		bucketName = "peerdb_staging"
	}

	content, err := e2e.ReadFileToBytes(credsPath)
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
	return &S3TestHelper{
		client,
		&protos.S3Config{
			Url:             fmt.Sprintf("s3://%s/%s", bucketName, prefixName),
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
) ([]*s3.Object, error) {

	Bucket := h.bucketName
	Prefix := fmt.Sprintf("%s/%s/", prefixName, jobName)
	files, err := h.client.ListObjects(&s3.ListObjectsInput{
		Bucket: &Bucket,
		Prefix: &Prefix,
	})
	if err != nil {
		log.Errorf("failed to list bucket files: %v", err)
		return nil, err
	}
	log.Infof("Files in ListAllFiles in S3 test: %v", files)
	return files.Contents, nil
}

// Delete all generated objects during the test
func (h *S3TestHelper) CleanUp() error {
	Bucket := h.bucketName
	Prefix := prefixName
	files, err := h.client.ListObjects(&s3.ListObjectsInput{
		Bucket: &Bucket,
		Prefix: &Prefix,
	})
	if err != nil {
		log.Errorf("failed to list bucket files: %v", err)
		return err
	}

	// Delete each object
	for _, obj := range files.Contents {
		deleteInput := &s3.DeleteObjectInput{
			Bucket: aws.String(Bucket),
			Key:    obj.Key,
		}

		_, err := h.client.DeleteObject(deleteInput)
		if err != nil {
			return err
		}
	}

	log.Infof("Deletion completed.")
	return nil
}
