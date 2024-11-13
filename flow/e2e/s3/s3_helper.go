package e2e_s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type S3TestHelper struct {
	client     *s3.Client
	S3Config   *protos.S3Config
	BucketName string
	prefix     string
}

type S3Environment int

const (
	Aws S3Environment = iota
	Gcs
	Minio
)

func NewS3TestHelper(s3environment S3Environment) (*S3TestHelper, error) {
	var config utils.S3PeerCredentials
	var endpoint string
	var credsPath string
	var bucketName string
	switch s3environment {
	case Aws:
		credsPath = os.Getenv("TEST_S3_CREDS")
		bucketName = "peerdb-test-bucket"
	case Gcs:
		credsPath = os.Getenv("TEST_GCS_CREDS")
		bucketName = "peerdb_staging"
		endpoint = "https://storage.googleapis.com"
	case Minio:
		bucketName = "peerdb"
		endpoint = os.Getenv("AWS_ENDPOINT_URL_S3")
		config.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
		config.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		config.Region = os.Getenv("AWS_REGION")
	default:
		panic(fmt.Sprintf("invalid s3environment %d", s3environment))
	}

	if credsPath != "" {
		content, err := e2eshared.ReadFileToBytes(credsPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		if err := json.Unmarshal(content, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
	}

	var endpointUrlPtr *string
	if endpoint != "" {
		endpointUrlPtr = &endpoint
	}
	provider := utils.NewStaticAWSCredentialsProvider(utils.AWSCredentials{
		AWS: aws.Credentials{
			AccessKeyID:     config.AccessKeyID,
			SecretAccessKey: config.SecretAccessKey,
			SessionToken:    config.SessionToken,
		},
		EndpointUrl: endpointUrlPtr,
	}, config.Region)
	client, err := utils.CreateS3Client(context.Background(), provider)
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
			Endpoint:        endpointUrlPtr,
		},
		bucketName,
		prefix,
	}, nil
}

// List all files from the S3 bucket.
// returns as a list of S3Objects.
func (h *S3TestHelper) ListAllFiles(
	ctx context.Context,
	jobName string,
) ([]s3types.Object, error) {
	Prefix := fmt.Sprintf("%s/%s/", h.prefix, jobName)
	files, err := h.client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &h.BucketName,
		Prefix: &Prefix,
	})
	if err != nil {
		return nil, err
	}
	return files.Contents, nil
}

// Delete all generated objects during the test
func (h *S3TestHelper) CleanUp(ctx context.Context) error {
	files, err := h.client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: &h.BucketName,
		Prefix: &h.prefix,
	})
	if err != nil {
		return err
	}

	// Delete each object
	for _, obj := range files.Contents {
		if _, err := h.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &h.BucketName,
			Key:    obj.Key,
		}); err != nil {
			return err
		}
	}

	return nil
}
