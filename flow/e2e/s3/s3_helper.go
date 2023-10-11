package e2e_s3

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const (
	peerName   string = "test_s3_peer"
	bucketName string = "peerdb-test-bucket"
	prefixName string = "test-s3"
)

type S3TestHelper struct {
	client   *s3.S3
	s3Config *protos.S3Config
}

func NewS3TestHelper() (*S3TestHelper, error) {
	client, err := utils.CreateS3Client(utils.S3PeerCredentials{})
	if err != nil {
		return nil, err
	}
	log.Infof("S3 client obtained")
	return &S3TestHelper{
		client,
		&protos.S3Config{
			Url: fmt.Sprintf("s3://%s/%s", bucketName, prefixName),
		},
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
	Bucket := bucketName
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
	Bucket := bucketName
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
