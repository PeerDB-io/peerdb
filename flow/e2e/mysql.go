package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

type MySqlSource struct {
	*connmysql.MySqlConnector
	Config *protos.MySqlConfig
}

func SetupMySQL(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()
	myVersion := os.Getenv("CI_MYSQL_VERSION")
	if myVersion == "" {
		t.Skip()
	}
	mode, isMySQL := strings.CutPrefix(myVersion, "mysql-")
	replicationMode := protos.MySqlReplicationMechanism_MYSQL_GTID
	if mode == "pos" {
		replicationMode = protos.MySqlReplicationMechanism_MYSQL_FILEPOS
	}
	if !isMySQL && myVersion != "maria" {
		t.Error("unknown mysql version", myVersion)
	}
	return SetupMyCore(t, suffix, !isMySQL, replicationMode)
}

func SetupMyCore(t *testing.T, suffix string, isMaria bool, replicationMechanism protos.MySqlReplicationMechanism) (*MySqlSource, error) {
	t.Helper()

	config := &protos.MySqlConfig{
		Host:                 "localhost",
		Port:                 3306,
		User:                 "root",
		Password:             "cipass",
		Database:             "",
		Setup:                nil,
		Compression:          0,
		DisableTls:           true,
		Flavor:               protos.MySqlFlavor_MYSQL_MYSQL,
		ReplicationMechanism: replicationMechanism,
	}
	if isMaria {
		config.Flavor = protos.MySqlFlavor_MYSQL_MARIA
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
		if _, err := connector.Execute(t.Context(), "select get_lock('settings',-1)"); err != nil {
			connector.Close()
			return nil, err
		}
		rs, err := connector.Execute(t.Context(), "select @@gtid_mode")
		if err != nil {
			connector.Close()
			return nil, err
		}
		gtidMode, err := rs.GetString(0, 0)
		if err != nil {
			connector.Close()
			return nil, err
		}
		if !strings.EqualFold(gtidMode, "on") {
			for _, sql := range []string{
				"set global binlog_row_metadata=full",
				"set global enforce_gtid_consistency=on",
				"set global gtid_mode=off_permissive",
				"set global gtid_mode=on_permissive",
				"set global gtid_mode=on",
			} {
				if _, err := connector.Execute(t.Context(), sql); err != nil {
					connector.Close()
					return nil, err
				}
			}
		}
		if _, err := connector.Execute(t.Context(), "do release_lock('settings')"); err != nil {
			connector.Close()
			return nil, err
		}
	} else {
		for _, sql := range []string{
			"set global binlog_format=row",
			"set binlog_format=row",
			"set global binlog_row_metadata=full",
		} {
			if _, err := connector.Execute(t.Context(), sql); err != nil {
				connector.Close()
				return nil, err
			}
		}
	}

	return &MySqlSource{MySqlConnector: connector, Config: config}, nil
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

	peer := &protos.Peer{
		Name: "mysql",
		Type: protos.DBType_MYSQL,
		Config: &protos.Peer_MysqlConfig{
			MysqlConfig: s.Config,
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
		fmt.Sprintf(`SELECT %s FROM "e2e_test_%s".%s ORDER BY id`, cols, suffix, utils.QuoteIdentifier(table)),
	)
	if err != nil {
		return nil, err
	}

	tableName := fmt.Sprintf("e2e_test_%s.%s", suffix, table)
	tableSchemas, err := s.GetTableSchema(ctx, nil, protos.TypeSystem_Q, []*protos.TableMapping{{SourceTableIdentifier: tableName}})
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
