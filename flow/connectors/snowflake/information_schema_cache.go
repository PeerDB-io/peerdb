package connsnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	schemaExistsSQL = "SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.SCHEMATA WHERE UPPER(SCHEMA_NAME)=?"
	tableExistsSQL  = `SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA)=? and UPPER(TABLE_NAME)=?`
	tableSchemaSQL  = `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=? AND UPPER(TABLE_NAME)=? ORDER BY ORDINAL_POSITION`
	tableSchemasInSchema = `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=? ORDER BY TABLE_NAME, ORDINAL_POSITION`
)

type informationSchemaCache struct {
	tableSchemaCache  *expirable.LRU[string, *protos.TableSchema]
	schemaExistsCache *expirable.LRU[string, bool]
	tableExistsCache  *expirable.LRU[string, bool]
}

func newInformationSchemaCache() *informationSchemaCache {
	schemaExistsCache := expirable.NewLRU[string, bool](100_000, nil, time.Hour*1)
	tableExistsCache := expirable.NewLRU[string, bool](100_000, nil, time.Hour*1)

	tsCacheExpiry := peerdbenv.PeerDBSnowflakeTableSchemaCacheSeconds()
	tableSchemaCache := expirable.NewLRU[string, *protos.TableSchema](100_000, nil, tsCacheExpiry)

	return &informationSchemaCache{
		tableSchemaCache:  tableSchemaCache,
		schemaExistsCache: schemaExistsCache,
		tableExistsCache:  tableExistsCache,
	}
}

var (
	cache     *informationSchemaCache
	cacheInit sync.Once
)

type SnowflakeInformationSchemaCache struct {
	ctx      context.Context
	database *sql.DB
	logger   slog.Logger
}

func NewSnowflakeInformationSchemaCache(
	ctx context.Context,
	database *sql.DB,
	logger slog.Logger,
) *SnowflakeInformationSchemaCache {
	cacheInit.Do(func() {
		cache = newInformationSchemaCache()
	})
	return &SnowflakeInformationSchemaCache{
		ctx:      ctx,
		database: database,
		logger:   logger,
	}
}

// SchemaExists returns true if the schema exists in the database.
func (c *SnowflakeInformationSchemaCache) SchemaExists(schemaName string) (bool, error) {
	schemaName = strings.ToUpper(schemaName)

	if cachedExists, ok := cache.schemaExistsCache.Get(schemaName); ok {
		if cachedExists {
			return true, nil
		}
	}

	// If a schema doesn't exist in the cache, lets fall back to the database.
	row := c.database.QueryRowContext(c.ctx, schemaExistsSQL, schemaName)

	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error querying Snowflake peer for schema %s: %w", schemaName, err)
	}

	if exists {
		err = c.cacheAllTablesInSchema(schemaName)
		if err != nil {
			return false, fmt.Errorf("error caching all tables in schema %s: %w", schemaName, err)
		}
	}

	cache.schemaExistsCache.Add(schemaName, exists)

	return exists, nil
}

// TableExists returns true if the table exists in the database.
func (c *SnowflakeInformationSchemaCache) TableExists(schemaName string, tableName string) (bool, error) {
	schemaName = strings.ToUpper(schemaName)
	tableName = strings.ToUpper(tableName)

	schemaTable := &utils.SchemaTable{
		Schema: schemaName,
		Table:  tableName,
	}

	if cachedExists, ok := cache.tableExistsCache.Get(schemaTable.String()); ok {
		if cachedExists {
			return true, nil
		}
	}

	row := c.database.QueryRowContext(c.ctx, tableExistsSQL, schemaTable.Schema, schemaTable.Table)

	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error querying Snowflake peer for table %s: %w", tableName, err)
	}

	cache.tableExistsCache.Add(schemaTable.String(), exists)

	return exists, nil
}

func (c *SnowflakeInformationSchemaCache) TableSchemaForTable(tableName string) (*protos.TableSchema, error) {
	tableName = strings.ToUpper(tableName)

	schemaTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("error while parsing table schema and name: %w", err)
	}

	if cachedSchema, ok := cache.tableSchemaCache.Get(schemaTable.String()); ok {
		return cachedSchema, nil
	}

	rows, err := c.database.QueryContext(c.ctx, tableSchemaSQL, schemaTable.Schema, schemaTable.Table)
	if err != nil {
		return nil, fmt.Errorf("error querying Snowflake peer for schema of table %s: %w", tableName, err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			c.logger.Error("error while closing rows for reading schema of table",
				slog.String("tableName", tableName),
				slog.Any("error", err))
		}
	}()

	var columnName, columnType sql.NullString
	columnNames := make([]string, 0, 8)
	columnTypes := make([]string, 0, 8)
	for rows.Next() {
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return nil, fmt.Errorf("error reading row for schema of table %s: %w", tableName, err)
		}

		genericColType, err := snowflakeTypeToQValueKind(columnType.String)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}

		columnNames = append(columnNames, columnName.String)
		columnTypes = append(columnTypes, string(genericColType))
	}

	tblSchema := &protos.TableSchema{
		TableIdentifier: tableName,
		ColumnNames:     columnNames,
		ColumnTypes:     columnTypes,
	}

	cache.tableSchemaCache.Add(schemaTable.String(), tblSchema)

	return tblSchema, nil
}

// cacheAllTablesInSchema caches all tables in a schema as exists.
func (c *SnowflakeInformationSchemaCache) cacheAllTablesInSchema(schemaName string) error {
	c.logger.Info("[snowflake] caching all table schemas in schema", slog.String("schemaName", schemaName))

	schemaName = strings.ToUpper(schemaName)

	rows, err := c.database.QueryContext(c.ctx, tableSchemasInSchema, schemaName)
	if err != nil {
		return fmt.Errorf("error querying Snowflake peer for schema of table %s: %w", schemaName, err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			c.logger.Error("error while closing rows for reading schema of table",
				slog.String("schemaName", schemaName),
				slog.Any("error", err))
		}
	}()

	// current schema for a given table.
	cs := make(map[string]*protos.TableSchema)

	var tableName, columnName, columnType sql.NullString
	for rows.Next() {
		err = rows.Scan(&tableName, &columnName, &columnType)
		if err != nil {
			return fmt.Errorf("error reading row for schema of table %s: %w", schemaName, err)
		}

		genericColType, err := snowflakeTypeToQValueKind(columnType.String)
		if err != nil {
			genericColType = qvalue.QValueKindString
		}

		if _, ok := cs[tableName.String]; !ok {
			schemaTable := &utils.SchemaTable{
				Schema: schemaName,
				Table:  strings.ToUpper(tableName.String),
			}

			cache.tableExistsCache.Add(schemaTable.String(), true)

			cs[tableName.String] = &protos.TableSchema{
				TableIdentifier: tableName.String,
				ColumnNames:     make([]string, 0, 8),
				ColumnTypes:     make([]string, 0, 8),
			}
		}

		cs[tableName.String].ColumnNames = append(cs[tableName.String].ColumnNames, columnName.String)
		cs[tableName.String].ColumnTypes = append(cs[tableName.String].ColumnTypes, string(genericColType))
	}

	for _, tblSchema := range cs {
		cache.tableSchemaCache.Add(tblSchema.TableIdentifier, tblSchema)
	}

	return nil
}
