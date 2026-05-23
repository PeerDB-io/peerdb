package objectstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// stagingCheckObjectPrefix is the basename used for temporary objects written
// during a staging-bucket smoke test. Kept short and unambiguous so it's easy
// to spot in bucket listings if a test object is ever leaked.
const stagingCheckObjectPrefix = "_peerdb_check_"

type StagingValidator = func(ctx context.Context) error

func NoStagingValidator(ctx context.Context) error {
	return nil
}

// NewS3StagingValidator returns a StagingValidator that smoke-tests an S3
// bucket by writing a small object under prefix and then deleting it. Callers
// supply a constructed *s3.Client and the bucket+prefix to test.
func NewS3StagingValidator(client *s3.Client, bucket, prefix string) StagingValidator {
	return func(ctx context.Context) error {
		key := strings.TrimPrefix(prefix+"/"+stagingCheckObjectPrefix+uuid.NewString(), "/")
		body := strings.NewReader(time.Now().Format(time.RFC3339))

		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   body,
		}); err != nil {
			return fmt.Errorf("failed to write to bucket: %w", err)
		}

		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			return fmt.Errorf("failed to delete from bucket: %w", err)
		}

		return nil
	}
}

// NewGCSStagingValidator returns a StagingValidator that smoke-tests a GCS
// bucket by writing a small object under prefix and then deleting it.
func NewGCSStagingValidator(client *storage.Client, bucket, prefix string) StagingValidator {
	return func(ctx context.Context) error {
		key := strings.TrimPrefix(prefix+"/"+stagingCheckObjectPrefix+uuid.NewString(), "/")
		obj := client.Bucket(bucket).Object(key)

		w := obj.NewWriter(ctx)
		if _, err := w.Write([]byte(time.Now().Format(time.RFC3339))); err != nil {
			w.Close()
			return fmt.Errorf("failed to write test object to GCS: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to finalize test object in GCS: %w", err)
		}

		if err := obj.Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete test object from GCS: %w", err)
		}

		return nil
	}
}
