package connclickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
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
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseConnector struct {
	*metadataStore.PostgresMetadata
	database           *sql.DB
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
func (c *ClickhouseConnector) ValidateCheck(ctx context.Context) error {
	validateDummyTableName := "peerdb_validation_" + shared.RandomString(4)
	// create a table
	_, err := c.database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id UInt64) ENGINE = Memory",
		validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}

	// insert a row
	_, err = c.database.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableName, err)
	}

	// drop the table
	_, err = c.database.ExecContext(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName)
	if err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableName, err)
	}

	validateErr := ValidateS3(ctx, c.credsProvider)
	if validateErr != nil {
		return fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
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

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		logger.Error("failed to create postgres metadata store", "error", err)
		return nil, err
	}

	credentialsProvider, err := utils.GetAWSCredentialsProvider(ctx, "clickhouse", utils.PeerAWSCredentials{
		Credentials: aws.Credentials{
			AccessKeyID:     config.AccessKeyId,
			SecretAccessKey: config.SecretAccessKey,
		},
		EndpointUrl: nil,
		Region:      config.Region,
	})
	if err != nil {
		return nil, err
	}

	awsBucketPath := config.S3Path

	if awsBucketPath == "" {
		deploymentUID := peerdbenv.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s",
			url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Fallback: Get S3 credentials from environment
		awsBucketName := peerdbenv.PeerDBClickhouseAWSS3BucketName()
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
	if err != nil {
		return nil, err
	}
	if credentials.AWS.SessionToken != "" {
		// This is the minimum version of Clickhouse that actually supports session token
		// https://github.com/ClickHouse/ClickHouse/issues/61230
		minSupportedClickhouseVersion := "v24.3.1"
		clickHouseVersionRow := database.QueryRowContext(ctx, "SELECT version()")
		var clickHouseVersion string
		err := clickHouseVersionRow.Scan(&clickHouseVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to query clickhouse version: %w", err)
		}
		// Ignore everything after patch version and prefix with "v", else semver.Compare will fail
		versionParts := strings.SplitN(clickHouseVersion, ".", 4)
		if len(versionParts) > 3 {
			versionParts = versionParts[:3]
		}
		cleanedClickHouseVersion := "v" + strings.Join(versionParts, ".")
		if semver.Compare(cleanedClickHouseVersion, minSupportedClickhouseVersion) < 0 {
			return nil, fmt.Errorf(
				"provide AWS access credentials explicitly or upgrade to clickhouse version >= %v, current version is %s. %s",
				minSupportedClickhouseVersion, clickHouseVersion,
				"You can also contact PeerDB support for implicit S3 setup for older versions of Clickhouse.")
		}
	}

	validateErr := ValidateS3(ctx, &clickHouseS3CredentialsNew)
	if validateErr != nil {
		return nil, fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	return &ClickhouseConnector{
		database:           database,
		PostgresMetadata:   pgMetadata,
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
