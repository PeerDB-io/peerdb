package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

type MySqlSource struct {
	*connmysql.MySqlConnector
	IsMaria bool
}

var mysqlConfig = &protos.MySqlConfig{
	Host:        "localhost",
	Port:        3306,
	User:        "root",
	Password:    "cipass",
	Database:    "",
	Setup:       nil,
	Compression: 0,
	DisableTls:  true,
	Flavor:      protos.MySqlFlavor_MYSQL_MYSQL,
}

var mariaConfig = &protos.MySqlConfig{
	Host:        "localhost",
	Port:        3300,
	User:        "root",
	Password:    "cipass",
	Database:    "",
	Setup:       nil,
	Compression: 0,
	DisableTls:  true,
	Flavor:      protos.MySqlFlavor_MYSQL_MARIA,
}

func SetupMySQL(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()
	return setupMyCore(t, suffix, false)
}

func SetupMariaDB(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()
	t.Skip("skipping until working out how to not have port conflict in GH actions")
	return setupMyCore(t, suffix, true)
}

func setupMyCore(t *testing.T, suffix string, isMaria bool) (*MySqlSource, error) {
	t.Helper()

	config := mysqlConfig
	if isMaria {
		config = mariaConfig
	}

	connector, err := connmysql.NewMySqlConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %w", err)
	}

	if _, err := connector.Execute(
		t.Context(), fmt.Sprintf("DROP DATABASE IF EXISTS \"e2e_test_%s\"", suffix),
	); err != nil {
		connector.Close()
		return nil, err
	}

	if _, err := connector.Execute(
		t.Context(), fmt.Sprintf("CREATE DATABASE \"e2e_test_%s\"", suffix),
	); err != nil {
		connector.Close()
		return nil, err
	}

	if !isMaria {
		if _, err := connector.Execute(t.Context(), "set global binlog_row_metadata=full"); err != nil {
			connector.Close()
			return nil, err
		}
	}

	return &MySqlSource{MySqlConnector: connector, IsMaria: isMaria}, nil
}

func (s *MySqlSource) Connector() connectors.Connector {
	return s.MySqlConnector
}

func (s *MySqlSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	if _, err := s.MySqlConnector.Execute(
		ctx, fmt.Sprintf("DROP DATABASE IF EXISTS \"e2e_test_%s\"", suffix),
	); err != nil {
		t.Log("failed to drop mysql database", err)
		s.MySqlConnector.Close()
	}
}

func (s *MySqlSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	config := mysqlConfig
	if s.IsMaria {
		config = mariaConfig
	}

	name := "mysql"
	if config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		name = "maria"
	}

	peer := &protos.Peer{
		Name: name,
		Type: protos.DBType_MYSQL,
		Config: &protos.Peer_MysqlConfig{
			MysqlConfig: config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *MySqlSource) Exec(ctx context.Context, sql string) error {
	_, err := s.MySqlConnector.Execute(ctx, sql)
	return err
}

func (s *MySqlSource) GetRows(ctx context.Context, suffix string, table string, cols string) (*model.QRecordBatch, error) {
	rs, err := s.MySqlConnector.Execute(
		ctx,
		fmt.Sprintf(`SELECT %s FROM "e2e_test_%s".%s ORDER BY id`, cols, suffix, connpostgres.QuoteIdentifier(table)),
	)
	if err != nil {
		return nil, err
	}

	tableName := fmt.Sprintf("e2e_test_%s.%s", suffix, table)
	tableSchemas, err := s.GetTableSchema(ctx, nil, protos.TypeSystem_Q, []string{tableName})
	if err != nil {
		return nil, err
	}

	schema, err := connmysql.QRecordSchemaFromMysqlFields(tableSchemas[tableName], rs.Fields)
	if err != nil {
		return nil, err
	}

	batch := &model.QRecordBatch{
		Schema:  schema,
		Records: nil,
	}

	for _, row := range rs.Values {
		record := make([]qvalue.QValue, 0, len(row))
		for idx, val := range row {
			qv, err := connmysql.QValueFromMysqlFieldValue(schema.Fields[idx].Type, val)
			if err != nil {
				return nil, err
			}
			record = append(record, qv)
		}
		batch.Records = append(batch.Records, record)
	}

	return batch, nil
}
