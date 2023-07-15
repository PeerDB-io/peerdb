package connsqlserver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	_ "github.com/microsoft/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

type SQLServerConnector struct {
	ctx          context.Context
	config       *protos.SqlServerConfig
	tableSchemas map[string]*protos.TableSchema
	db           *sql.DB
}

// NewSQLServerConnector creates a new SQL Server connection
func NewSQLServerConnector(ctx context.Context, config *protos.SqlServerConfig) (*SQLServerConnector, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		config.Server, config.User, config.Password, config.Port, config.Database)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &SQLServerConnector{
		ctx:    ctx,
		config: config,
		db:     db,
	}, nil
}

// Close closes the database connection
func (c *SQLServerConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ConnectionActive checks if the connection is still active
func (c *SQLServerConnector) ConnectionActive() bool {
	if err := c.db.Ping(); err != nil {
		return false
	}
	return true
}

func (c *SQLServerConnector) NeedsSetupMetadataTables() bool {
	log.Errorf("NeedsSetupMetadataTables not supported for SQLServer")
	return false
}

func (c *SQLServerConnector) SetupMetadataTables() error {
	log.Errorf("SetupMetadataTables not supported for SQLServer")
	return fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) GetLastOffset(jobName string) (*protos.LastSyncState, error) {
	log.Errorf("GetLastOffset not supported for SQLServer")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) GetLastNormalizeBatchID() (int64, error) {
	log.Errorf("GetLastNormalizeBatchID not supported for SQLServer")
	return 0, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) GetTableSchema(req *protos.GetTableSchemaInput) (*protos.TableSchema, error) {
	log.Errorf("GetTableSchema not supported for SQLServer flow connector")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) SetupNormalizedTable(
	req *protos.SetupNormalizedTableInput) (*protos.SetupNormalizedTableOutput, error) {
	log.Errorf("SetupNormalizedTable not supported for SQLServer")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) InitializeTableSchema(req map[string]*protos.TableSchema) error {
	log.Errorf("InitializeTableSchema not supported for SQLServer")
	return fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) PullRecords(req *model.PullRecordsRequest) (*model.RecordBatch, error) {
	log.Errorf("panicking at call to PullRecords for SQLServer flow connector")
	panic("PullRecords is not implemented for the SQLServer flow connector")
}

func (c *SQLServerConnector) SyncRecords(req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	log.Errorf("SyncRecords not supported for SQLServer")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) NormalizeRecords(req *model.NormalizeRecordsRequest) (*model.NormalizeResponse, error) {
	log.Errorf("NormalizeRecords not supported for SQLServer")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) CreateRawTable(req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	log.Errorf("CreateRawTable not supported for SQLServer")
	return nil, fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}

func (c *SQLServerConnector) EnsurePullability(req *protos.EnsurePullabilityInput,
) (*protos.EnsurePullabilityOutput, error) {
	log.Errorf("panicking at call to EnsurePullability for SQLServer flow connector")
	panic("EnsurePullability is not implemented for the SQLServer flow connector")
}

func (c *SQLServerConnector) SetupReplication(req *protos.SetupReplicationInput) error {
	log.Errorf("panicking at call to SetupReplication for SQLServer flow connector")
	panic("SetupReplication is not implemented for the SQLServer flow connector")
}

func (c *SQLServerConnector) PullFlowCleanup(jobName string) error {
	log.Errorf("panicking at call to PullFlowCleanup for SQLServer flow connector")
	panic("PullFlowCleanup is not implemented for the SQLServer flow connector")
}

func (c *SQLServerConnector) SyncFlowCleanup(jobName string) error {
	log.Errorf("SyncFlowCleanup not supported for SQLServer")
	return fmt.Errorf("cdc based replication is not currently supported for SQLServer target")
}
