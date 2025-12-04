package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	var replicationMode protos.MySqlReplicationMechanism
	var mysqlFlavor protos.MySqlFlavor
	switch myVersion {
	case "mysql-gtid":
		replicationMode = protos.MySqlReplicationMechanism_MYSQL_GTID
		mysqlFlavor = protos.MySqlFlavor_MYSQL_MYSQL
	case "mysql-pos":
		replicationMode = protos.MySqlReplicationMechanism_MYSQL_FILEPOS
		mysqlFlavor = protos.MySqlFlavor_MYSQL_MYSQL
	case "maria":
		replicationMode = protos.MySqlReplicationMechanism_MYSQL_GTID
		mysqlFlavor = protos.MySqlFlavor_MYSQL_MARIA
	default:
		t.Error("unexpected mysql version", myVersion)
	}
	return SetupMyCore(t, suffix, replicationMode, mysqlFlavor)
}

func SetupMyCore(t *testing.T, suffix string, replication protos.MySqlReplicationMechanism, flavor protos.MySqlFlavor) (*MySqlSource, error) {
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
		Flavor:               flavor,
		ReplicationMechanism: replication,
	}

	connector, err := connmysql.NewMySqlConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql connection: %w", err)
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

	setupSql := []string{
		"set global binlog_format=row",
		"set global binlog_row_image=full",
		"set global max_connections=500",
	}

	if cmp, err := connector.CompareServerVersion(t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata); err != nil {
		t.Fatal(err)
	} else if cmp >= 0 {
		setupSql = append(setupSql, "set global binlog_row_metadata=full")
	}

	if flavor != protos.MySqlFlavor_MYSQL_MARIA {
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
		if replication == protos.MySqlReplicationMechanism_MYSQL_GTID {
			if strings.EqualFold(gtidMode, "off") {
				// The value of @@GLOBAL.GTID_MODE can only be changed one step at a time:
				// OFF <-> OFF_PERMISSIVE <-> ON_PERMISSIVE <-> ON
				setupSql = append(setupSql,
					"select get_lock('settings',-1)",
					"set global enforce_gtid_consistency=on",
					"set global gtid_mode=off_permissive",
					"set global gtid_mode=on_permissive",
					"set global gtid_mode=on",
					"do release_lock('settings')",
				)
			}
		} else if replication == protos.MySqlReplicationMechanism_MYSQL_FILEPOS {
			if strings.EqualFold(gtidMode, "on") {
				// The value of @@GLOBAL.GTID_MODE can only be changed one step at a time:
				// ON <-> ON_PERMISSIVE <-> OFF_PERMISSIVE <-> OFF
				setupSql = append(setupSql,
					"select get_lock('settings',-1)",
					"set global enforce_gtid_consistency=off",
					"set global gtid_mode=on_permissive",
					"set global gtid_mode=off_permissive",
					"set global gtid_mode=off",
					"do release_lock('settings')",
				)
			}
		} else {
			return nil, fmt.Errorf("unexpected replication mechanism: %v", replication)
		}
	}

	for _, sql := range setupSql {
		if _, err := connector.Execute(t.Context(), sql); err != nil {
			connector.Close()
			return nil, fmt.Errorf("error executing %s: %v", sql, err)
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
		fmt.Sprintf(`SELECT %s FROM "e2e_test_%s".%s ORDER BY id`, cols, suffix, common.QuoteIdentifier(table)),
	)
	if err != nil {
		return nil, err
	}

	tableName := fmt.Sprintf("e2e_test_%s.%s", suffix, table)
	tableSchemas, err := s.GetTableSchema(ctx, nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
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
		record := make([]types.QValue, 0, len(row))
		for idx, val := range row {
			qv, err := connmysql.QValueFromMysqlFieldValue(schema.Fields[idx].Type, rs.Fields[idx].Type, val)
			if err != nil {
				return nil, err
			}
			record = append(record, qv)
		}
		batch.Records = append(batch.Records, record)
	}

	return batch, nil
}
