package connsnowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/snowflakedb/gosnowflake"

	peersql "github.com/PeerDB-io/peer-flow/connectors/sql"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	util "github.com/PeerDB-io/peer-flow/utils"
)

type SnowflakeClient struct {
	peersql.GenericSQLQueryExecutor
	// ctx is the context.
	ctx context.Context
	// config is the Snowflake config.
	Config *protos.SnowflakeConfig
}

func NewSnowflakeClient(ctx context.Context, config *protos.SnowflakeConfig) (*SnowflakeClient, error) {
	privateKey, err := util.DecodePKCS8PrivateKey([]byte(config.PrivateKey), config.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	snowflakeConfig := gosnowflake.Config{
		Account:          config.AccountId,
		User:             config.Username,
		Authenticator:    gosnowflake.AuthTypeJwt,
		PrivateKey:       privateKey,
		Database:         config.Database,
		Warehouse:        config.Warehouse,
		Role:             config.Role,
		RequestTimeout:   time.Duration(config.QueryTimeout) * time.Second,
		DisableTelemetry: true,
	}

	snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get DSN from Snowflake config: %w", err)
	}

	database, err := sqlx.Open("snowflake", snowflakeConfigDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	err = database.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	genericExecutor := *peersql.NewGenericSQLQueryExecutor(
		ctx, database, snowflakeTypeToQValueKindMap, qvalue.QValueKindToSnowflakeTypeMap)

	return &SnowflakeClient{
		GenericSQLQueryExecutor: genericExecutor,
		ctx:                     ctx,
		Config:                  config,
	}, nil
}

func (c *SnowflakeConnector) getTableCounts(tables []string) (int64, error) {
	var totalRecords int64
	for _, table := range tables {
		parsedSchemaTable, err := utils.ParseSchemaTable(table)
		if err != nil {
			return 0, fmt.Errorf("failed to parse table name %s: %w", table, err)
		}
		//nolint:gosec
		row := c.database.QueryRowContext(c.ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s",
			snowflakeSchemaTableNormalize(parsedSchemaTable)))
		var count int64
		err = row.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to get count for table %s: %w", table, err)
		}
		totalRecords += count
	}
	return totalRecords, nil
}

func snowflakeIdentifierNormalize(identifier string) string {
	// https://www.alberton.info/dbms_identifiers_and_case_sensitivity.html
	// Snowflake follows the SQL standard, but Postgres does the opposite.
	// Ergo, we suffer.
	if strings.ToLower(identifier) == identifier {
		return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
	}
	return fmt.Sprintf(`"%s"`, identifier)
}

func snowflakeSchemaTableNormalize(schemaTable *utils.SchemaTable) string {
	return fmt.Sprintf(`%s.%s`, snowflakeIdentifierNormalize(schemaTable.Schema),
		snowflakeIdentifierNormalize(schemaTable.Table))
}
