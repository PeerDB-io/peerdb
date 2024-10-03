package connclickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.temporal.io/sdk/log"
	"golang.org/x/mod/semver"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickHouseConnector struct {
	*metadataStore.PostgresMetadata
	database      clickhouse.Conn
	logger        log.Logger
	config        *protos.ClickhouseConfig
	credsProvider *utils.ClickHouseS3Credentials
	s3Stage       *ClickHouseS3Stage
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := c.exec(ctx, "DROP TABLE IF EXISTS "+validateDummyTableName); err != nil {
			c.logger.Error("validation failed to drop table", slog.String("table", validateDummyTableName), slog.Any("error", err))
		}
	}()

	// add a column
	if err := c.exec(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN updated_at DateTime64(9) DEFAULT now64()", validateDummyTableName),
	); err != nil {
		return fmt.Errorf("failed to add column to validation table %s: %w", validateDummyTableName, err)
	}

	// rename the table
	if err := c.exec(ctx,
		fmt.Sprintf("RENAME TABLE %s TO %s", validateDummyTableName, validateDummyTableName+"_renamed"),
	); err != nil {
		return fmt.Errorf("failed to rename validation table %s: %w", validateDummyTableName, err)
	}
	validateDummyTableName += "_renamed"

	// insert a row
	if err := c.exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, now64())", validateDummyTableName)); err != nil {
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
	logger := logger.LoggerFromCtx(ctx)
	database, err := Connect(ctx, config)
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
		bucketPathSuffix := fmt.Sprintf("%s/%s",
			url.PathEscape(deploymentUID), url.PathEscape(flowName))
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
	clickHouseS3CredentialsNew := utils.ClickHouseS3Credentials{
		Provider:   credentialsProvider,
		BucketPath: awsBucketPath,
	}
	credentials, err := credentialsProvider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	if credentials.AWS.SessionToken != "" {
		// This is the minimum version of ClickHouse that actually supports session token
		// https://github.com/ClickHouse/ClickHouse/issues/61230
		minSupportedClickHouseVersion := "v24.3.1"
		clickHouseVersionRow := database.QueryRow(ctx, "SELECT version()")
		var clickHouseVersion string
		err := clickHouseVersionRow.Scan(&clickHouseVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to query ClickHouse version: %w", err)
		}
		// Ignore everything after patch version and prefix with "v", else semver.Compare will fail
		versionParts := strings.SplitN(clickHouseVersion, ".", 4)
		if len(versionParts) > 3 {
			versionParts = versionParts[:3]
		}
		cleanedClickHouseVersion := "v" + strings.Join(versionParts, ".")
		if semver.Compare(cleanedClickHouseVersion, minSupportedClickHouseVersion) < 0 {
			return nil, fmt.Errorf(
				"provide S3 Transient Stage details explicitly or upgrade to ClickHouse version >= %v, current version is %s. %s",
				minSupportedClickHouseVersion, clickHouseVersion,
				"You can also contact PeerDB support for implicit S3 stage setup for older versions of ClickHouse.")
		}
	}

	return &ClickHouseConnector{
		database:         database,
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           logger,
		credsProvider:    &clickHouseS3CredentialsNew,
		s3Stage:          NewClickHouseS3Stage(),
	}, nil
}

func Connect(ctx context.Context, config *protos.ClickhouseConfig) (clickhouse.Conn, error) {
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

// https://github.com/ClickHouse/clickhouse-kafka-connect/blob/2e0c17e2f900d29c00482b9d0a1f55cb678244e5/src/main/java/com/clickhouse/kafka/connect/util/Utils.java#L78-L93
//
//nolint:lll
var retryableExceptions = map[int32]struct{}{
	3:   {}, // UNEXPECTED_END_OF_FILE
	107: {}, // FILE_DOESNT_EXIST
	159: {}, // TIMEOUT_EXCEEDED
	164: {}, // READONLY
	202: {}, // TOO_MANY_SIMULTANEOUS_QUERIES
	203: {}, // NO_FREE_CONNECTION
	209: {}, // SOCKET_TIMEOUT
	210: {}, // NETWORK_ERROR
	241: {}, // MEMORY_LIMIT_EXCEEDED
	242: {}, // TABLE_IS_READ_ONLY
	252: {}, // TOO_MANY_PARTS
	285: {}, // TOO_FEW_LIVE_REPLICAS
	319: {}, // UNKNOWN_STATUS_OF_INSERT
	425: {}, // SYSTEM_ERROR
	999: {}, // KEEPER_EXCEPTION
}

func isRetryableException(err error) bool {
	if ex, ok := err.(*clickhouse.Exception); ok {
		if ex == nil {
			return false
		}
		_, yes := retryableExceptions[ex.Code]
		return yes
	}
	return errors.Is(err, io.EOF)
}

//nolint:unparam
func (c *ClickHouseConnector) exec(ctx context.Context, query string, args ...any) error {
	var err error
	for i := range 5 {
		err = c.database.Exec(ctx, query, args...)
		if !isRetryableException(err) {
			break
		}
		c.logger.Info("[exec] retryable error", slog.Any("error", err), slog.Any("query", query), slog.Int64("i", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return err
}

func (c *ClickHouseConnector) query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	var rows driver.Rows
	var err error
	for i := range 5 {
		rows, err = c.database.Query(ctx, query, args...)
		if !isRetryableException(err) {
			break
		}
		c.logger.Info("[query] retryable error", slog.Any("error", err), slog.Any("query", query), slog.Int64("i", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return rows, err
}

func (c *ClickHouseConnector) queryRow(ctx context.Context, query string, args ...any) driver.Row {
	var row driver.Row
	for i := range 5 {
		row = c.database.QueryRow(ctx, query, args...)
		err := row.Err()
		if !isRetryableException(err) {
			break
		}
		c.logger.Info("[queryRow] retryable error", slog.Any("error", row.Err()), slog.Any("query", query), slog.Int64("i", int64(i)))
		if i < 4 {
			time.Sleep(time.Second * time.Duration(i*5+1))
		}
	}
	return row
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
	return c.database.Exec(ctx, query)
}

func (c *ClickHouseConnector) checkTablesEmptyAndEngine(ctx context.Context, tables []string) error {
	queryInput := make([]interface{}, 0, len(tables)+1)
	queryInput = append(queryInput, c.config.Database)
	for _, table := range tables {
		queryInput = append(queryInput, table)
	}
	rows, err := c.query(ctx,
		fmt.Sprintf("SELECT name,engine,total_rows FROM system.tables WHERE database=? AND table IN (%s)",
			strings.Join(slices.Repeat([]string{"?"}, len(tables)), ",")), queryInput...)
	if err != nil {
		return fmt.Errorf("failed to get information for destination tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, engine string
		var totalRows uint64
		err = rows.Scan(&tableName, &engine, &totalRows)
		if err != nil {
			return fmt.Errorf("failed to scan information for tables: %w", err)
		}
		if totalRows != 0 {
			return fmt.Errorf("table %s exists and is not empty", tableName)
		}
		if !slices.Contains(acceptableTableEngines, strings.TrimPrefix(engine, "Shared")) {
			c.logger.Warn("[clickhouse] table engine not explicitly supported",
				slog.String("table", tableName), slog.String("engine", engine))
		}
	}
	if rows.Err() != nil {
		return fmt.Errorf("failed to read rows: %w", rows.Err())
	}
	return nil
}

func (c *ClickHouseConnector) getTableColumnsMapping(ctx context.Context,
	tables []string,
) (map[string][]*protos.FieldDescription, error) {
	tableColumnsMapping := make(map[string][]*protos.FieldDescription, len(tables))
	queryInput := make([]interface{}, 0, len(tables)+1)
	queryInput = append(queryInput, c.config.Database)
	for _, table := range tables {
		queryInput = append(queryInput, table)
	}
	rows, err := c.query(ctx,
		fmt.Sprintf("SELECT name,type,table FROM system.columns WHERE database=? AND table IN (%s)",
			strings.Join(slices.Repeat([]string{"?"}, len(tables)), ",")), queryInput...)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for destination tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var fieldDescription protos.FieldDescription
		err = rows.Scan(&fieldDescription.Name, &fieldDescription.Type, &tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan columns for tables: %w", err)
		}
		tableColumnsMapping[tableName] = append(tableColumnsMapping[tableName], &fieldDescription)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to read rows: %w", rows.Err())
	}
	return tableColumnsMapping, nil
}

func (c *ClickHouseConnector) processTableComparison(dstTableName string, srcSchema *protos.TableSchema,
	dstSchema []*protos.FieldDescription, peerDBColumns []string, tableMapping *protos.TableMapping,
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
		err := c.checkTablesEmptyAndEngine(ctx, dstTableNames)
		if err != nil {
			return err
		}
	}
	// optimization: fetching columns for all tables at once
	chTableColumnsMapping, err := c.getTableColumnsMapping(ctx, dstTableNames)
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
