package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmssql "github.com/PeerDB-io/peerdb/flow/connectors/mssql"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type MsSqlSource struct {
	*connmssql.MsSqlConnector
	Config *protos.SqlServerConfig
	conn   *sql.DB
}

func mssqlHost() string {
	if host := os.Getenv("CI_MSSQL_HOST"); host != "" {
		return host
	}
	return "localhost"
}

func mssqlPort() uint32 {
	return 1433
}

func SetupMsSql(t *testing.T, suffix string) (*MsSqlSource, error) {
	t.Helper()

	if os.Getenv("CI_MSSQL_HOST") == "" {
		return nil, fmt.Errorf("CI_MSSQL_HOST not set, skipping SQL Server tests")
	}

	config := &protos.SqlServerConfig{
		Host:     mssqlHost(),
		Port:     mssqlPort(),
		User:     "sa",
		Password: "PeerDB@ci1234",
		Database: "",
	}

	connector, err := connmssql.NewMsSqlConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create mssql connection: %w", err)
	}

	// create a raw sql.DB for setup commands (connector's conn targets no specific DB)
	connStr := "sqlserver://" + config.User + ":" + config.Password +
		"@" + config.Host + ":" + strconv.FormatUint(uint64(config.Port), 10)

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		connector.Close()
		return nil, fmt.Errorf("failed to open mssql db: %w", err)
	}

	dbName := "e2e_test_" + suffix

	// drop if exists, create fresh
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf(
		"IF DB_ID('%s') IS NOT NULL BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s]; END",
		dbName, dbName, dbName)); err != nil {
		db.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to drop database: %w", err)
	}
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE [%s]", dbName)); err != nil {
		db.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// enable CDC on database
	if _, err := db.ExecContext(t.Context(), fmt.Sprintf("USE [%s]; EXEC sys.sp_cdc_enable_db", dbName)); err != nil {
		db.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to enable CDC on database: %w", err)
	}

	db.Close()

	// reconnect targeting the new database
	config.Database = dbName
	connector.Close()
	connector, err = connmssql.NewMsSqlConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to reconnect to mssql: %w", err)
	}

	// open a raw db handle for Exec/GetRows
	connStrDB := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
		config.User, config.Password, config.Host, config.Port, dbName)
	rawDB, err := sql.Open("sqlserver", connStrDB)
	if err != nil {
		connector.Close()
		return nil, fmt.Errorf("failed to open mssql db: %w", err)
	}

	return &MsSqlSource{MsSqlConnector: connector, Config: config, conn: rawDB}, nil
}

func (s *MsSqlSource) Connector() connectors.Connector {
	return s.MsSqlConnector
}

func (s *MsSqlSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	dbName := "e2e_test_" + suffix
	s.conn.Close()
	// reconnect to master to drop
	connStr := "sqlserver://" + s.Config.User + ":" + s.Config.Password +
		"@" + s.Config.Host + ":" + strconv.FormatUint(uint64(s.Config.Port), 10)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Log("failed to connect for teardown", err)
		return
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"IF DB_ID('%s') IS NOT NULL BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [%s]; END",
		dbName, dbName, dbName)); err != nil {
		t.Log("failed to drop mssql database", err)
	}
}

func (s *MsSqlSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: s.Config.Database,
		Type: protos.DBType_SQLSERVER,
		Config: &protos.Peer_SqlserverConfig{
			SqlserverConfig: s.Config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *MsSqlSource) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.conn.ExecContext(ctx, sql, args...)
	return err
}

// EnableTableCdc enables CDC on a table, retrying on deadlock.
func (s *MsSqlSource) EnableTableCdc(ctx context.Context, schema, table string) error {
	return s.MsSqlConnector.RetryOnDeadlock(func() error {
		_, err := s.conn.ExecContext(ctx, fmt.Sprintf(
			"EXEC sys.sp_cdc_enable_table @source_schema=N'%s', @source_name=N'%s', @role_name=NULL, @supports_net_changes=0",
			schema, table))
		return err
	})
}

func (s *MsSqlSource) GetRows(ctx context.Context, suffix string, table string, cols string) (*model.QRecordBatch, error) {
	query := "SELECT " + cols + " FROM [e2e_test_" + suffix + "].[dbo].[" + table + "] ORDER BY id" //nolint:gosec // test code
	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	schema := types.QRecordSchema{Fields: make([]types.QField, 0, len(colTypes))}
	qkinds := make([]types.QValueKind, len(colTypes))
	for i, ct := range colTypes {
		qkind, err := connmssql.QkindFromMssqlColumnType(ct.DatabaseTypeName())
		if err != nil {
			return nil, err
		}
		qkinds[i] = qkind
		nullable, _ := ct.Nullable()
		schema.Fields = append(schema.Fields, types.QField{
			Name:     ct.Name(),
			Nullable: nullable,
			Type:     qkind,
		})
	}

	batch := &model.QRecordBatch{Schema: schema}
	for rows.Next() {
		scanArgs := make([]any, len(colTypes))
		for i := range scanArgs {
			scanArgs[i] = new(any)
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		record := make([]types.QValue, 0, len(scanArgs))
		for i, sa := range scanArgs {
			rawVal := *(sa.(*any))
			qv, err := connmssql.QValueFromMssqlValue(qkinds[i], rawVal)
			if err != nil {
				return nil, err
			}
			record = append(record, qv)
		}
		batch.Records = append(batch.Records, record)
	}
	return batch, rows.Err()
}
