package connpostgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

// PostgresConnector is a Connector implementation for Postgres.
type PostgresConnector struct {
	connStr string
	ctx     context.Context
	config  *protos.PostgresConfig
	pool    *pgxpool.Pool
}

// SchemaTable is a table in a schema.
type SchemaTable struct {
	Schema string
	Table  string
}

func (t *SchemaTable) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// NewPostgresConnector creates a new instance of PostgresConnector.
func NewPostgresConnector(ctx context.Context, pgConfig *protos.PostgresConfig) (*PostgresConnector, error) {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		pgConfig.Host, pgConfig.Port, pgConfig.User, pgConfig.Password, pgConfig.Database)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return &PostgresConnector{
		connStr: connectionString,
		ctx:     ctx,
		config:  pgConfig,
		pool:    pool,
	}, nil
}

// Close closes all connections.
func (c *PostgresConnector) Close() error {
	if c.pool != nil {
		c.pool.Close()
	}

	return nil
}

// ConnectionActive returns true if the connection is active.
func (c *PostgresConnector) ConnectionActive() bool {
	return c.pool != nil
}

// NeedsSetupMetadataTables returns true if the metadata tables need to be set up.
func (c *PostgresConnector) NeedsSetupMetadataTables() bool {
	return false
}

// SetupMetadataTables sets up the metadata tables.
func (c *PostgresConnector) SetupMetadataTables() error {
	panic("not implemented")
}

// GetLastOffset returns the last synced offset for a job.
func (c *PostgresConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	panic("not implemented")
}

func (c *PostgresConnector) GetLastSyncBatchID(jobName string) (int64, error) {
	panic("not implemented")
}

func (c *PostgresConnector) GetLastNormalizeBatchID(jobName string) (int64, error) {
	panic("not implemented")
}
func (c *PostgresConnector) GetDistinctTableNamesInBatch(flowJobName string, syncBatchID int64,
	normalizeBatchID int64) ([]string, error) {
	panic("not implemented")
}

// PullRecords pulls records from the source.
func (c *PostgresConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)

	// ensure that replication is set to database
	connConfig, err := pgxpool.ParseConfig(c.connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	connConfig.ConnConfig.RuntimeParams["replication"] = "database"

	replPool, err := pgxpool.NewWithConfig(c.ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	cdc, err := NewPostgresCDCSource(&PostrgesCDCConfig{
		AppContext:            c.ctx,
		Connection:            replPool,
		SrcTableIDNameMapping: req.SrcTableIDNameMapping,
		Slot:                  slotName,
		Publication:           publicationName,
		TableNameMapping:      req.TableNameMapping,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc source: %w", err)
	}

	defer cdc.Close()

	return cdc.PullRecords(req)
}

// SyncRecords pushes records to the destination.
func (c *PostgresConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	panic("not implemented")
}

func (c *PostgresConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	panic("not implemented")
}

type SlotCheckResult struct {
	SlotExists        bool
	PublicationExists bool
}

// checkSlotAndPublication checks if the replication slot and publication exist.
func (c *PostgresConnector) checkSlotAndPublication(slot string, publication string) (*SlotCheckResult, error) {
	slotExists := false
	publicationExists := false

	// Check if the replication slot exists
	var slotName string
	err := c.pool.QueryRow(c.ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&slotName)
	if err != nil {
		// check if the error is a "no rows" error
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("error checking for replication slot - %s: %w", slot, err)
		}
	} else {
		slotExists = true
	}

	// Check if the publication exists
	var pubName string
	err = c.pool.QueryRow(c.ctx,
		"SELECT pubname FROM pg_publication WHERE pubname = $1",
		publication).Scan(&pubName)
	if err != nil {
		// check if the error is a "no rows" error
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("error checking for publication - %s: %w", publication, err)
		}
	} else {
		publicationExists = true
	}

	return &SlotCheckResult{
		SlotExists:        slotExists,
		PublicationExists: publicationExists,
	}, nil
}

// createSlotAndPublication creates the replication slot and publication.
func (c *PostgresConnector) createSlotAndPublication(
	s *SlotCheckResult,
	slot string,
	publication string,
	tableNameMapping map[string]string,
) error {
	if !s.SlotExists {
		// Create the logical replication slot
		_, err := c.pool.Exec(c.ctx,
			"SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')",
			slot)
		if err != nil {
			return fmt.Errorf("error creating replication slot: %w", err)
		}
	}
	/*
		iterating through source tables and creating a publication.
		expecting tablenames to be schema qualified
	*/
	srcTableNames := make([]string, 0, len(tableNameMapping))
	for srcTableName := range tableNameMapping {
		if len(strings.Split(srcTableName, ".")) != 2 {
			return fmt.Errorf("source tables identifier is invalid: %v", srcTableName)
		}
		srcTableNames = append(srcTableNames, srcTableName)
	}
	tableNameString := strings.Join(srcTableNames, ", ")

	if !s.PublicationExists {
		// Create the publication to help filter changes only for the given tables
		stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publication, tableNameString)
		_, err := c.pool.Exec(c.ctx, stmt)
		if err != nil {
			return fmt.Errorf("error creating publication: %w", err)
		}
	}

	return nil
}

// CreateRawTable creates a raw table, implementing the Connector interface.
func (c *PostgresConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	panic("not implemented")
}

// getRelIDForTable returns the relation ID for a table.
func (c *PostgresConnector) getRelIDForTable(schemaTable *SchemaTable) (uint32, error) {
	var relID uint32
	err := c.pool.QueryRow(c.ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n
		 ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`,
		strings.ToLower(schemaTable.Schema), strings.ToLower(schemaTable.Table)).Scan(&relID)
	if err != nil {
		return 0, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}

	return relID, nil
}

// GetTableSchema returns the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	schemaTable, err := parseSchemaTable(req.TableIdentifier)
	if err != nil {
		return nil, err
	}

	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, err)
	}

	// Get the column names and types
	rows, err := c.pool.Query(c.ctx,
		`SELECT a.attname, t.typname FROM pg_attribute a
		 JOIN pg_type t ON t.oid = a.atttypid
		 WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = $1`,
		relID)
	if err != nil {
		return nil, fmt.Errorf("error getting table schema for table %s: %w", schemaTable, err)
	}
	defer rows.Close()

	pkey, err := c.getPrimaryKeyColumn(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
	}

	res := &protos.TableSchema{
		TableIdentifier:  req.TableIdentifier,
		Columns:          make(map[string]string),
		PrimaryKeyColumn: pkey,
	}

	for rows.Next() {
		var colName string
		var colType string
		err = rows.Scan(&colName, &colType)
		if err != nil {
			return nil, fmt.Errorf("error scanning table schema: %w", err)
		}

		colType, err = convertPostgresColumnTypeToGeneric(colType)
		if err != nil {
			return nil, fmt.Errorf("error converting postgres column type: %w", err)
		}

		res.Columns[colName] = colType
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table schema: %w", err)
	}

	return res, nil
}

// SetupNormalizedTable sets up a normalized table, implementing the Connector interface.
func (c *PostgresConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput,
) (*protos.SetupNormalizedTableOutput, error) {
	panic("not implemented")
}

// InitializeTableSchema initializes the schema for a table, implementing the Connector interface.
func (c *PostgresConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	panic("not implemented")
}

// EnsurePullability ensures that a table is pullable, implementing the Connector interface.
func (c *PostgresConnector) EnsurePullability(req *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	schemaTable, err := parseSchemaTable(req.SourceTableIdentifier)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema and table: %w", err)
	}

	// check if the table exists by getting the relation ID
	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return nil, fmt.Errorf("error getting relation ID for table %s: %w", schemaTable, err)
	}
	return &protos.EnsurePullabilityOutput{TableIdentifier: &protos.TableIdentifier{
		TableIdentifier: &protos.TableIdentifier_PostgresTableIdentifier{
			PostgresTableIdentifier: &protos.PostgresTableIdentifier{
				RelId: relID},
		},
	}}, nil
}

// SetupReplication sets up replication for the source connector.
func (c *PostgresConnector) SetupReplication(req *protos.SetupReplicationInput) error {
	//schemaTable, err := parseSchemaTable(req.SourceTableIdentifier)

	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", req.FlowJobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", req.FlowJobName)

	// Check if the replication slot and publication exist
	exists, err := c.checkSlotAndPublication(slotName, publicationName)
	if err != nil {
		return fmt.Errorf("error checking for replication slot and publication: %w", err)
	}

	// Create the replication slot and publication
	err = c.createSlotAndPublication(exists, slotName, publicationName, req.TableNameMapping)
	if err != nil {
		return fmt.Errorf("error creating replication slot and publication: %w", err)
	}
	return nil
}

func (c *PostgresConnector) PullFlowCleanup(jobName string) error {
	// Slotname would be the job name prefixed with "peerflow_slot_"
	slotName := fmt.Sprintf("peerflow_slot_%s", jobName)

	// Publication name would be the job name prefixed with "peerflow_pub_"
	publicationName := fmt.Sprintf("peerflow_pub_%s", jobName)

	pullFlowCleanupTx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction for flow cleanup: %w", err)
	}
	defer func() {
		deferErr := pullFlowCleanupTx.Rollback(c.ctx)
		if deferErr != pgx.ErrTxClosed && deferErr != nil {
			log.Errorf("unexpected error rolling back transaction for flow cleanup: %v", err)
		}
	}()
	_, err = pullFlowCleanupTx.Exec(c.ctx, fmt.Sprintf("DROP PUBLICATION %s", publicationName))
	if err != nil {
		return fmt.Errorf("error dropping publication: %w", err)
	}
	_, err = pullFlowCleanupTx.Exec(c.ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	if err != nil {
		return fmt.Errorf("error dropping replication slot: %w", err)
	}
	err = pullFlowCleanupTx.Commit(c.ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction for flow cleanup: %w", err)
	}

	return nil
}

func (c *PostgresConnector) SyncFlowCleanup(jobName string) error {
	panic("not implemented")
}

// parseSchemaTable parses a table name into schema and table name.
func parseSchemaTable(tableName string) (*SchemaTable, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	return &SchemaTable{
		Schema: parts[0],
		Table:  parts[1],
	}, nil
}

// getPrimaryKeyColumn for table returns the primary key column for a given table
// errors if there is no primary key column or if there is more than one primary key column.
func (c *PostgresConnector) getPrimaryKeyColumn(schemaTable *SchemaTable) (string, error) {
	relID, err := c.getRelIDForTable(schemaTable)
	if err != nil {
		return "", fmt.Errorf("failed to get relation id for table %s: %w", schemaTable, err)
	}

	// Get the primary key column name
	var pkCol string
	err = c.pool.QueryRow(c.ctx,
		`SELECT a.attname FROM pg_index i
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indrelid = $1 AND i.indisprimary`,
		relID).Scan(&pkCol)
	if err != nil {
		return "", fmt.Errorf("error getting primary key column for table %s: %w", schemaTable, err)
	}

	return pkCol, nil
}

func convertPostgresColumnTypeToGeneric(colType string) (string, error) {
	switch colType {
	case "int2":
		return model.ColumnTypeInt16, nil
	case "int4":
		return model.ColumnTypeInt32, nil
	case "int8":
		return model.ColumnTypeInt64, nil
	case "float4":
		return model.ColumnTypeFloat32, nil
	case "float8":
		return model.ColumnTypeFloat64, nil
	case "bool":
		return model.ColumnTypeBoolean, nil
	case "text":
		return model.ColumnTypeString, nil
	case "date":
		return model.ColumnTypeDate, nil
	case "timestamp":
		return model.ColumnTypeTimestamp, nil
	case "timestamptz":
		return model.ColumnTypeTimeStampWithTimeZone, nil
	case "varchar":
		return model.ColumnTypeString, nil
	case "char":
		return model.ColumnTypeString, nil
	case "bpchar":
		return model.ColumnTypeString, nil
	case "numeric":
		return model.ColumnTypeNumeric, nil
	case "uuid":
		return model.ColumnTypeString, nil
	case "json":
		return model.ColumnTypeJSON, nil
	case "jsonb":
		return model.ColumnTypeJSON, nil
	case "xml":
		return model.ColumnTypeString, nil
	case "tsvector":
		return model.ColumnTypeString, nil
	case "tsquery":
		return model.ColumnTypeString, nil
	case "bytea":
		return model.ColumnTypeBytes, nil
	case "bit":
		return model.ColumnTypeBytes, nil
	case "varbit":
		return model.ColumnTypeBytes, nil
	case "cidr":
		return model.ColumnTypeString, nil
	case "inet":
		return model.ColumnTypeString, nil
	case "interval":
		return model.ColumnTypeInterval, nil
	case "macaddr":
		return model.ColumnTypeString, nil
	case "money":
		return model.ColumnTypeFloat64, nil
	case "oid":
		return model.ColumnTypeInt64, nil
	case "time":
		return model.ColumnTypeTime, nil
	case "timetz":
		return model.ColumnTypeTimeWithTimeZone, nil
	case "txid_snapshot":
		return model.ColumnTypeString, nil
	default:
		return "", fmt.Errorf("unsupported column type: %s", colType)
	}
}
