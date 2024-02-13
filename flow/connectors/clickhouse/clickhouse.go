package connclickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
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
	creds              *utils.ClickhouseS3Credentials
}

func ValidateS3(ctx context.Context, creds *utils.ClickhouseS3Credentials) error {
	// for validation purposes
	s3Client, err := utils.CreateS3Client(utils.S3PeerCredentials{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		Region:          creds.Region,
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	validErr := conns3.ValidCheck(ctx, s3Client, creds.BucketPath, nil)
	if validErr != nil {
		return validErr
	}

	return nil
}

// Creates and drops a dummy table to validate the peer
func ValidateClickhouse(ctx context.Context, conn *sql.DB) error {
	validateDummyTableName := fmt.Sprintf("peerdb_validation_%s", shared.RandomString(4))
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
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", validateDummyTableName))
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

	var clickhouseS3Creds *utils.ClickhouseS3Credentials
	deploymentUID := shared.GetDeploymentUID()
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	bucketPathSuffix := fmt.Sprintf("%s/%s",
		url.PathEscape(deploymentUID), url.PathEscape(flowName))

	// Get user provided S3 credentials
	clickhouseS3Creds = &utils.ClickhouseS3Credentials{
		AccessKeyID:     config.AccessKeyId,
		SecretAccessKey: config.SecretAccessKey,
		Region:          config.Region,
		BucketPath:      config.S3Path,
	}

	if clickhouseS3Creds.AccessKeyID == "" &&
		clickhouseS3Creds.SecretAccessKey == "" && clickhouseS3Creds.Region == "" &&
		clickhouseS3Creds.BucketPath == "" {
		// Fallback: Get S3 credentials from environment
		clickhouseS3Creds = utils.GetClickhouseAWSSecrets(bucketPathSuffix)
	}

	validateErr := ValidateS3(ctx, clickhouseS3Creds)
	if validateErr != nil {
		return nil, fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	return &ClickhouseConnector{
		database:           database,
		pgMetadata:         pgMetadata,
		tableSchemaMapping: nil,
		config:             config,
		creds:              clickhouseS3Creds,
		logger:             logger,
	}, nil
}

func connect(ctx context.Context, config *protos.ClickhouseConfig) (*sql.DB, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.User,
			Password: config.Password,
		},
		TLS:         &tls.Config{MinVersion: tls.VersionTLS13},
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

func (c *ClickhouseConnector) Close(_ context.Context) error {
	if c == nil || c.database == nil {
		return nil
	}

	err := c.database.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive(ctx context.Context) error {
	if c == nil || c.database == nil {
		return fmt.Errorf("ClickhouseConnector is nil")
	}

	// This also checks if database exists
	err := c.database.PingContext(ctx)
	return err
}
