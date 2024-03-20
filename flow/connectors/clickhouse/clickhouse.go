package connclickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/log"
	"golang.org/x/mod/semver"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseConnector struct {
	database           *sql.DB
	pgMetadata         *metadataStore.PostgresMetadataStore
	tableSchemaMapping map[string]*protos.TableSchema
	logger             log.Logger
	config             *protos.ClickhouseConfig
	credsProvider      *utils.ClickHouseS3Credentials
}

func ValidateS3(ctx context.Context, creds *utils.ClickHouseS3Credentials) error {
	// for validation purposes
	s3Client, err := utils.CreateS3Client(ctx, creds.Provider)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	object, err := utils.NewS3BucketAndPrefix(creds.BucketPath)
	if err != nil {
		return fmt.Errorf("failed to create S3 bucket and prefix: %w", err)
	}

	prefix := object.Prefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	_, listErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &object.Bucket,
		Prefix: &prefix,
	},
	)
	if listErr != nil {
		return fmt.Errorf("failed to list objects: %w", listErr)
	}

	return nil
}

// Creates and drops a dummy table to validate the peer
func ValidateClickhouse(ctx context.Context, conn *sql.DB) error {
	validateDummyTableName := "peerdb_validation_" + shared.RandomString(4)
	// create a table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id UInt64) ENGINE = Memory",
		validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}

	// insert a row
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableName, err)
	}

	// drop the table
	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName)
	if err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableName, err)
	}

	return nil
}

func NewClickhouseConnector(
	ctx context.Context,
	config *protos.ClickhouseConfig,
) (*ClickhouseConnector, error) {
	logger := logger.LoggerFromCtx(ctx)
	database, err := connect(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	err = ValidateClickhouse(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("invalidated Clickhouse peer: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	credentialsProvider, err := utils.GetAWSCredentialsProvider(ctx, "clickhouse", utils.PeerAWSCredentials{
		Credentials: aws.Credentials{
			AccessKeyID:     config.AccessKeyId,
			SecretAccessKey: config.SecretAccessKey,
		},
		Region:      config.Region,
		EndpointUrl: nil,
	})
	if err != nil {
		return nil, err
	}

	awsBucketPath := config.S3Path

	if awsBucketPath == "" {
		deploymentUID := shared.GetDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s",
			url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Fallback: Get S3 credentials from environment
		awsBucketName := os.Getenv("PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME")
		if awsBucketName == "" {
			return nil, errors.New("PeerDB Clickhouse Bucket Name not set")
		}

		awsBucketPath = fmt.Sprintf("s3://%s/%s", awsBucketName, bucketPathSuffix)
	}
	clickHouseS3CredentialsNew := utils.ClickHouseS3Credentials{
		Provider:   credentialsProvider,
		BucketPath: awsBucketPath,
	}
	credentials, err := credentialsProvider.Retrieve(ctx)
	// TODO finish and test this flow when a compatible ClickHouse version is available
	if credentials.AWS.SessionToken != "" {
		minSupportedClickhouseVersions := []string{
			// TODO versions having
			// https://github.com/ClickHouse/ClickHouse/commit/d045ab150ed5659f143beca5e50d0b72dae3bf78
		}
		clickHouseVersionRow := database.QueryRowContext(ctx, "SELECT version()")
		var clickHouseVersion string
		err := clickHouseVersionRow.Scan(&clickHouseVersionRow)
		if err != nil {
			return nil, fmt.Errorf("failed to query clickhouse version: %w", err)
		}
		supportsSessionToken := false
		for _, minSupportedVersion := range minSupportedClickhouseVersions {
			minSupportedMajor := semver.Major(minSupportedVersion)
			currentClickHouseMajor := semver.Major(clickHouseVersion)
			// We are in the same major version, now check if we are ahead of the minor version having the change
			if semver.Compare(minSupportedMajor, currentClickHouseMajor) == 0 {
				if semver.Compare(minSupportedMajor, clickHouseVersion) <= 0 {
					supportsSessionToken = true
					break
				}
			}
		}
		if !supportsSessionToken {
			return nil, fmt.Errorf(
				"please provide AWS access credentials explicitly or upgrade to version >= %v, current version is %s",
				minSupportedClickhouseVersions, clickHouseVersion)
		}
	}

	if err != nil {
		return nil, err
	}

	validateErr := ValidateS3(ctx, &clickHouseS3CredentialsNew)
	if validateErr != nil {
		return nil, fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	return &ClickhouseConnector{
		database:           database,
		pgMetadata:         pgMetadata,
		tableSchemaMapping: nil,
		config:             config,
		logger:             logger,
		credsProvider:      &clickHouseS3CredentialsNew,
	}, nil
}

func connect(ctx context.Context, config *protos.ClickhouseConfig) (*sql.DB, error) {
	var tlsSetting *tls.Config
	if !config.DisableTls {
		tlsSetting = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.User,
			Password: config.Password,
		},
		TLS:         tlsSetting,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "peerdb"},
			},
		},
	})

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping to Clickhouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickhouseConnector) Close() error {
	if c != nil {
		err := c.database.Close()
		if err != nil {
			return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
		}
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive(ctx context.Context) error {
	// This also checks if database exists
	return c.database.PingContext(ctx)
}
