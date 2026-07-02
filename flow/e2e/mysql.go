package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connmysql "github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type MySqlSource struct {
	*connmysql.MySqlConnector
	Config *protos.MySqlConfig
	// peer name, defaults to "mysql"
	Name string
}

func SetupMySQL(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()
	flavor, replication := internal.MySQLTestFlavorAndMechanism(t)
	return setupMyConnector(t, suffix, internal.GetMySQLConfigFromEnv(flavor, replication), "")
}

func SetupMariaDB(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()
	flavor, replication := internal.MariaDBTestFlavorAndMechanism(t)
	return setupMyConnector(t, suffix, internal.GetMariaDBConfigFromEnv(flavor, replication), "mariadb")
}

// MySQLTestContainerConfig parameterizes a throwaway MySQL/MariaDB testcontainer source.
type MySQLTestContainerConfig struct {
	Image string
	// ExtraServerFlags are appended to a common small-footprint server flag base, e.g.
	// "--binlog-row-event-fragment-threshold=1024" (MariaDB) or "--mysqlx=0" (MySQL).
	ExtraServerFlags     []string
	Flavor               protos.MySqlFlavor
	ReplicationMechanism protos.MySqlReplicationMechanism
}

// SetupMySQLTestContainerSource starts a throwaway MySQL/MariaDB server in a testcontainer and
// returns a MySqlSource pointed at it, plus the generated suffix used for its e2e database. It
// registers container and database cleanup on t. Use it to replace the shared CI source with an
// isolated server a test can reconfigure (custom image, extra server flags) - typically by assigning
// the returned source's peer to flowConnConfig.SourceName.
func SetupMySQLTestContainerSource(
	t *testing.T, namePrefix string, cfg MySQLTestContainerConfig,
) (*MySqlSource, string) {
	t.Helper()

	rootPassword := internal.MySQLTestRootPasswordWithFallback("cipass")
	env := map[string]string{}
	if cfg.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		env["MARIADB_ROOT_PASSWORD"] = rootPassword
		env["MARIADB_ROOT_HOST"] = "%"
	} else {
		env["MYSQL_ROOT_PASSWORD"] = rootPassword
		env["MYSQL_ROOT_HOST"] = "%"
	}

	// Common small-footprint server flags valid on both MySQL 8 and MariaDB. Both official
	// entrypoints prepend the server binary when the first arg starts with "-", so none is needed.
	cmd := append([]string{
		"--server-id=1",
		"--log-bin=mysql-bin",
		"--binlog-format=ROW",
		"--innodb-buffer-pool-size=64M",
		"--performance-schema=OFF",
		"--max-connections=20",
	}, cfg.ExtraServerFlags...)

	req := testcontainers.ContainerRequest{
		Image:        cfg.Image,
		Env:          env,
		Cmd:          cmd,
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForListeningPort("3306/tcp").WithStartupTimeout(3 * time.Minute),
	}

	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	testcontainers.CleanupContainer(t, ctr, testcontainers.StopTimeout(30*time.Second))
	require.NoError(t, err)

	mapped, err := ctr.MappedPort(t.Context(), "3306/tcp")
	require.NoError(t, err)
	port, err := strconv.ParseUint(mapped.Port(), 10, 32)
	require.NoError(t, err)

	suffix := namePrefix + "_" + strings.ToLower(common.RandomString(8))
	config := &protos.MySqlConfig{
		// host.docker.internal resolves both from the test process (to the published port) and from
		// the flow worker container (via host-gateway), unlike the container's own hostname.
		Host:                 internal.MySQLTestHost(),
		Port:                 uint32(port),
		User:                 "root",
		Password:             rootPassword,
		DisableTls:           true,
		Flavor:               cfg.Flavor,
		ReplicationMechanism: cfg.ReplicationMechanism,
	}
	src, err := setupMyConnector(t, suffix, config, suffix)
	require.NoError(t, err)
	t.Cleanup(func() { src.Teardown(t, context.Background(), suffix) })

	return src, suffix
}

func setupMyConnector(t *testing.T, suffix string, config *protos.MySqlConfig, peerName string) (*MySqlSource, error) {
	t.Helper()
	connector, err := connmysql.NewMySqlConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql connection: %w", err)
	}

	dbName := "e2e_test_" + suffix
	for _, sql := range []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName),
		fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	} {
		if _, err := connector.Execute(t.Context(), sql); err != nil {
			connector.Close()
			return nil, err
		}
	}

	if err := connmysql.ConfigureReplication(t, connector, config); err != nil {
		connector.Close()
		return nil, err
	}

	return &MySqlSource{MySqlConnector: connector, Config: config, Name: peerName}, nil
}

func (s *MySqlSource) Connector() connectors.Connector {
	return s.MySqlConnector
}

func (s *MySqlSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	if _, err := s.MySqlConnector.Execute(
		ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `e2e_test_%s`", suffix),
	); err != nil {
		t.Log("failed to drop mysql database", err)
		s.MySqlConnector.Close()
	}
}

func (s *MySqlSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()

	name := s.Name
	if name == "" {
		name = "mysql"
	}
	peer := &protos.Peer{
		Name: name,
		Type: protos.DBType_MYSQL,
		Config: &protos.Peer_MysqlConfig{
			MysqlConfig: s.Config,
		},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *MySqlSource) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.MySqlConnector.Execute(ctx, sql, args...)
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

	schema, err := connmysql.QRecordSchemaFromMysqlFields(tableSchemas[tableName], rs.Fields, shared.InternalVersion_Latest)
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
