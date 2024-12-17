package connclickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	chvalidate "github.com/PeerDB-io/peer-flow/shared/clickhouse"
)

type ClickHouseConnector struct {
	*metadataStore.PostgresMetadata
	database      clickhouse.Conn
	logger        log.Logger
	config        *protos.ClickhouseConfig
	credsProvider *utils.ClickHouseS3Credentials
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

	return utils.PutAndRemoveS3(ctx, s3Client, object.Bucket, object.Prefix)
}

func ValidateClickHouseHost(ctx context.Context, chHost string, allowedDomainString string) error {
	allowedDomains := strings.Split(allowedDomainString, ",")
	if len(allowedDomains) == 0 {
		return nil
	}
	// check if chHost ends with one of the allowed domains
	for _, domain := range allowedDomains {
		if strings.HasSuffix(chHost, domain) {
			return nil
		}
	}
	return fmt.Errorf("invalid ClickHouse host domain: %s. Allowed domains: %s",
		chHost, strings.Join(allowedDomains, ","))
}

// Performs some checks on the ClickHouse peer to ensure it will work for mirrors
func (c *ClickHouseConnector) ValidateCheck(ctx context.Context) error {
	// validate clickhouse host
	allowedDomains := peerdbenv.PeerDBClickHouseAllowedDomains()
	if err := ValidateClickHouseHost(ctx, c.config.Host, allowedDomains); err != nil {
		return err
	}
	validateDummyTableName := "peerdb_validation_" + shared.RandomString(4)
	// create a table
	err := c.exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id UInt64
	) ENGINE = ReplacingMergeTree ORDER BY id;`,
		validateDummyTableName))
	if err != nil {
		return fmt.Errorf("failed to create validation table %s: %w", validateDummyTableName, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := c.exec(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName); err != nil {
			c.logger.Error("validation failed to drop table", slog.String("table", validateDummyTableName), slog.Any("error", err))
		}
	}()

	// add a column
	if err := c.exec(ctx,
		fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN updated_at DateTime64(9) DEFAULT now64()", validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to add column to validation table %s: %w", validateDummyTableName, err)
	}

	// rename the table
	if err := c.exec(ctx,
		fmt.Sprintf("RENAME TABLE `%s` TO `%s`", validateDummyTableName, validateDummyTableName+"_renamed"),
	); err != nil {
		return fmt.Errorf("failed to rename validation table %s: %w", validateDummyTableName, err)
	}
	validateDummyTableName += "_renamed"

	// insert a row
	if err := c.exec(ctx, fmt.Sprintf("INSERT INTO `%s` VALUES (1, now64())", validateDummyTableName)); err != nil {
		return fmt.Errorf("failed to insert into validation table %s: %w", validateDummyTableName, err)
	}

	// drop the table
	if err := c.exec(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName); err != nil {
		return fmt.Errorf("failed to drop validation table %s: %w", validateDummyTableName, err)
	}

	// validate s3 stage
	if err := ValidateS3(ctx, c.credsProvider); err != nil {
		return fmt.Errorf("failed to validate S3 bucket: %w", err)
	}

	return nil
}

func NewClickHouseConnector(
	ctx context.Context,
	env map[string]string,
	config *protos.ClickhouseConfig,
) (*ClickHouseConnector, error) {
	logger := shared.LoggerFromCtx(ctx)
	database, err := Connect(ctx, env, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to ClickHouse peer: %w", err)
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
		EndpointUrl: config.Endpoint,
		Region:      config.Region,
	})
	if err != nil {
		return nil, err
	}

	awsBucketPath := config.S3Path
	if awsBucketPath == "" {
		deploymentUID := peerdbenv.PeerDBDeploymentUID()
		flowName, _ := ctx.Value(shared.FlowNameKey).(string)
		bucketPathSuffix := fmt.Sprintf("%s/%s", url.PathEscape(deploymentUID), url.PathEscape(flowName))
		// Fallback: Get S3 credentials from environment
		awsBucketName, err := peerdbenv.PeerDBClickHouseAWSS3BucketName(ctx, env)
		if err != nil {
			return nil, fmt.Errorf("failed to get PeerDB ClickHouse Bucket Name: %w", err)
		}
		if awsBucketName == "" {
			return nil, errors.New("PeerDB ClickHouse Bucket Name not set")
		}

		awsBucketPath = fmt.Sprintf("s3://%s/%s", awsBucketName, bucketPathSuffix)
	}

	credentials, err := credentialsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	connector := &ClickHouseConnector{
		database:         database,
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           logger,
		credsProvider: &utils.ClickHouseS3Credentials{
			Provider:   credentialsProvider,
			BucketPath: awsBucketPath,
		},
	}

	if credentials.AWS.SessionToken != "" {
		// 24.3.1 is minimum version of ClickHouse that actually supports session token
		// https://github.com/ClickHouse/ClickHouse/issues/61230
		clickHouseVersion, err := database.ServerVersion()
		if err != nil {
			return nil, fmt.Errorf("failed to get ClickHouse version: %w", err)
		}
		if !chproto.CheckMinVersion(
			chproto.Version{Major: 24, Minor: 3, Patch: 1},
			clickHouseVersion.Version,
		) {
			return nil, fmt.Errorf(
				"provide S3 Transient Stage details explicitly or upgrade to ClickHouse version >= 24.3.1, current version is %s. %s",
				clickHouseVersion,
				"You can also contact PeerDB support for implicit S3 stage setup for older versions of ClickHouse.")
		}
	}

	return connector, nil
}

func Connect(ctx context.Context, env map[string]string, config *protos.ClickhouseConfig) (clickhouse.Conn, error) {
	var tlsSetting *tls.Config
	if !config.DisableTls {
		tlsSetting = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	if config.Certificate != nil || config.PrivateKey != nil {
		if config.Certificate == nil || config.PrivateKey == nil {
			return nil, errors.New("both certificate and private key must be provided if using certificate-based authentication")
		}
		cert, err := tls.X509KeyPair([]byte(*config.Certificate), []byte(*config.PrivateKey))
		if err != nil {
			return nil, fmt.Errorf("failed to parse provided certificate: %w", err)
		}
		tlsSetting.Certificates = []tls.Certificate{cert}
	}
	if config.RootCa != nil {
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM([]byte(*config.RootCa)) {
			return nil, errors.New("failed to parse provided root CA")
		}
		tlsSetting.RootCAs = caPool
	}

	// See: https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#consistency
	settings := clickhouse.Settings{"select_sequential_consistency": uint64(1)}
	if maxInsertThreads, err := peerdbenv.PeerDBClickHouseMaxInsertThreads(ctx, env); err != nil {
		return nil, fmt.Errorf("failed to load max_insert_threads config: %w", err)
	} else if maxInsertThreads != 0 {
		settings["max_insert_threads"] = maxInsertThreads
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
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
		Settings:    settings,
		DialTimeout: 3600 * time.Second,
		ReadTimeout: 3600 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse peer: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping to ClickHouse peer: %w", err)
	}

	return conn, nil
}

//nolint:unparam
func (c *ClickHouseConnector) exec(ctx context.Context, query string, args ...any) error {
	return chvalidate.Exec(ctx, c.logger, c.database, query, args...)
}

func (c *ClickHouseConnector) query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return chvalidate.Query(ctx, c.logger, c.database, query, args...)
}

func (c *ClickHouseConnector) queryRow(ctx context.Context, query string, args ...any) driver.Row {
	return chvalidate.QueryRow(ctx, c.logger, c.database, query, args...)
}

func (c *ClickHouseConnector) Close() error {
	if c != nil {
		err := c.database.Close()
		if err != nil {
			return fmt.Errorf("error while closing connection to ClickHouse peer: %w", err)
		}
	}
	return nil
}

func (c *ClickHouseConnector) ConnectionActive(ctx context.Context) error {
	// This also checks if database exists
	return c.database.Ping(ctx)
}

func (c *ClickHouseConnector) execWithLogging(ctx context.Context, query string) error {
	c.logger.Info("[clickhouse] executing DDL statement", slog.String("query", query))
	return c.exec(ctx, query)
}

func (c *ClickHouseConnector) processTableComparison(dstTableName string, srcSchema *protos.TableSchema,
	dstSchema []chvalidate.ClickHouseColumn, peerDBColumns []string, tableMapping *protos.TableMapping,
) error {
	for _, srcField := range srcSchema.Columns {
		colName := srcField.Name
		// if the column is mapped to a different name, find and use that name instead
		for _, col := range tableMapping.Columns {
			if col.SourceName == colName {
				if col.DestinationName != "" {
					colName = col.DestinationName
				}
				break
			}
		}
		found := false
		// compare either the source column name or the mapped destination column name to the ClickHouse schema
		for _, dstField := range dstSchema {
			// not doing type checks for now
			if dstField.Name == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field %s not found in destination table %s", srcField.Name, dstTableName)
		}
	}
	foundPeerDBColumns := 0
	for _, dstField := range dstSchema {
		// all these columns need to be present in the destination table
		if slices.Contains(peerDBColumns, dstField.Name) {
			foundPeerDBColumns++
		}
	}
	if foundPeerDBColumns != len(peerDBColumns) {
		return fmt.Errorf("not all PeerDB columns found in destination table %s", dstTableName)
	}
	return nil
}

func (c *ClickHouseConnector) CheckDestinationTables(ctx context.Context, req *protos.FlowConnectionConfigs,
	tableNameSchemaMapping map[string]*protos.TableSchema,
) error {
	if peerdbenv.PeerDBOnlyClickHouseAllowed() {
		err := chvalidate.CheckIfClickHouseCloudHasSharedMergeTreeEnabled(ctx, c.logger, c.database)
		if err != nil {
			return err
		}
	}

	peerDBColumns := []string{signColName, versionColName}
	if req.SyncedAtColName != "" {
		peerDBColumns = append(peerDBColumns, strings.ToLower(req.SyncedAtColName))
	}
	// this is for handling column exclusion, processed schema does that in a step
	processedMapping := shared.BuildProcessedSchemaMapping(req.TableMappings, tableNameSchemaMapping, c.logger)
	dstTableNames := slices.Collect(maps.Keys(processedMapping))

	// In the case of resync, we don't need to check the content or structure of the original tables;
	// they'll anyways get swapped out with the _resync tables which we CREATE OR REPLACE
	if !req.Resync {
		if err := chvalidate.CheckIfTablesEmptyAndEngine(ctx, c.logger, c.database,
			dstTableNames, req.DoInitialSnapshot, peerdbenv.PeerDBOnlyClickHouseAllowed()); err != nil {
			return err
		}
	}
	// optimization: fetching columns for all tables at once
	chTableColumnsMapping, err := chvalidate.GetTableColumnsMapping(ctx, c.logger, c.database, dstTableNames)
	if err != nil {
		return err
	}

	for _, tableMapping := range req.TableMappings {
		dstTableName := tableMapping.DestinationTableIdentifier
		if _, ok := processedMapping[dstTableName]; !ok {
			// if destination table is not a key, that means source table was not a key in the original schema mapping(?)
			return fmt.Errorf("source table %s not found in schema mapping", tableMapping.SourceTableIdentifier)
		}
		// if destination table does not exist, we're good
		if _, ok := chTableColumnsMapping[dstTableName]; !ok {
			continue
		}

		err = c.processTableComparison(dstTableName, processedMapping[dstTableName],
			chTableColumnsMapping[dstTableName], peerDBColumns, tableMapping)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseConnector) GetVersion(ctx context.Context) (string, error) {
	clickhouseVersion, err := c.database.ServerVersion()
	if err != nil {
		return "", fmt.Errorf("failed to get ClickHouse version: %w", err)
	}
	c.logger.Info("[clickhouse] version", slog.Any("version", clickhouseVersion.DisplayName))
	return clickhouseVersion.Version.String(), nil
}

func GetTableSchemaForTable(tableName string, columns []driver.ColumnType) (*protos.TableSchema, error) {
	colFields := make([]*protos.FieldDescription, 0, len(columns))
	for _, column := range columns {
		var qkind qvalue.QValueKind
		switch column.DatabaseTypeName() {
		case "String", "Nullable(String)":
			qkind = qvalue.QValueKindString
		case "Bool", "Nullable(Bool)":
			qkind = qvalue.QValueKindBoolean
		case "Int16", "Nullable(Int16)":
			qkind = qvalue.QValueKindInt16
		case "Int32", "Nullable(Int32)":
			qkind = qvalue.QValueKindInt32
		case "Int64", "Nullable(Int64)":
			qkind = qvalue.QValueKindInt64
		case "UUID", "Nullable(UUID)":
			qkind = qvalue.QValueKindUUID
		case "DateTime64(6)", "Nullable(DateTime64(6))":
			qkind = qvalue.QValueKindTimestamp
		case "Date32", "Nullable(Date32)":
			qkind = qvalue.QValueKindDate
		case "Float32", "Nullable(Float32)":
			qkind = qvalue.QValueKindFloat32
		case "Float64", "Nullable(Float64)":
			qkind = qvalue.QValueKindFloat64
		default:
			if strings.Contains(column.DatabaseTypeName(), "Decimal") {
				qkind = qvalue.QValueKindNumeric
			} else {
				return nil, fmt.Errorf("failed to resolve QValueKind for %s", column.DatabaseTypeName())
			}
		}

		colFields = append(colFields, &protos.FieldDescription{
			Name:         column.Name(),
			Type:         string(qkind),
			TypeModifier: -1,
			Nullable:     column.Nullable(),
		})
	}

	return &protos.TableSchema{
		TableIdentifier: tableName,
		Columns:         colFields,
		System:          protos.TypeSystem_Q,
	}, nil
}

func (c *ClickHouseConnector) GetTableSchema(
	ctx context.Context,
	_env map[string]string,
	_system protos.TypeSystem,
	tableIdentifiers []string,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableIdentifiers))
	for _, tableName := range tableIdentifiers {
		rows, err := c.database.Query(ctx, fmt.Sprintf("select * from %s limit 0", tableName))
		if err != nil {
			return nil, err
		}

		tableSchema, err := GetTableSchemaForTable(tableName, rows.ColumnTypes())
		rows.Close()
		if err != nil {
			return nil, err
		}
		res[tableName] = tableSchema
	}

	return res, nil
}
