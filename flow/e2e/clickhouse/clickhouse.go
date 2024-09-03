package e2e_clickhouse

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors"
	connclickhouse "github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	e2e_s3 "github.com/PeerDB-io/peer-flow/e2e/s3"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickHouseSuite struct {
	t        *testing.T
	conn     *connpostgres.PostgresConnector
	s3Helper *e2e_s3.S3TestHelper
	suffix   string
}

func (s ClickHouseSuite) T() *testing.T {
	return s.t
}

func (s ClickHouseSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s ClickHouseSuite) DestinationConnector() connectors.Connector {
	// TODO have CH connector
	return nil
}

func (s ClickHouseSuite) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s ClickHouseSuite) Suffix() string {
	return s.suffix
}

func (s ClickHouseSuite) Peer() *protos.Peer {
	return s.PeerForDatabase("e2e_test_" + s.suffix)
}

func (s ClickHouseSuite) PeerForDatabase(dbname string) *protos.Peer {
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, dbname),
		Type: protos.DBType_CLICKHOUSE,
		Config: &protos.Peer_ClickhouseConfig{
			ClickhouseConfig: &protos.ClickhouseConfig{
				Host:            "localhost",
				Port:            9000,
				Database:        dbname,
				S3Path:          s.s3Helper.BucketName,
				AccessKeyId:     *s.s3Helper.S3Config.AccessKeyId,
				SecretAccessKey: *s.s3Helper.S3Config.SecretAccessKey,
				Region:          *s.s3Helper.S3Config.Region,
				DisableTls:      true,
				Endpoint:        s.s3Helper.S3Config.Endpoint,
			},
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func (s ClickHouseSuite) DestinationTable(table string) string {
	return table
}

func (s ClickHouseSuite) Teardown() {
	require.NoError(s.t, s.s3Helper.CleanUp(context.Background()))
	e2e.TearDownPostgres(s)
}

func (s ClickHouseSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	ch, err := connclickhouse.Connect(context.Background(), s.Peer().GetClickhouseConfig())
	if err != nil {
		return nil, err
	}

	rows, err := ch.Query(
		context.Background(),
		fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s`, cols, table, strings.Split(cols, ",")[0]),
	)
	if err != nil {
		return nil, err
	}

	batch := &model.QRecordBatch{}
	types := rows.ColumnTypes()
	row := make([]interface{}, 0, len(types))
	for _, ty := range types {
		nullable := ty.Nullable()
		var qkind qvalue.QValueKind
		switch ty.DatabaseTypeName() {
		case "String":
			var val string
			row = append(row, &val)
			qkind = qvalue.QValueKindString
		case "Int32":
			var val int32
			row = append(row, &val)
			qkind = qvalue.QValueKindInt32
		default:
			return nil, fmt.Errorf("failed to resolve QValueKind for %s", ty.DatabaseTypeName())
		}
		batch.Schema.Fields = append(batch.Schema.Fields, qvalue.QField{
			Name:      ty.Name(),
			Type:      qkind,
			Precision: 0,
			Scale:     0,
			Nullable:  nullable,
		})
	}

	for rows.Next() {
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		qrow := make([]qvalue.QValue, 0, len(row))
		for _, val := range row {
			switch v := val.(type) {
			case *string:
				qrow = append(qrow, qvalue.QValueString{Val: *v})
			case *int32:
				qrow = append(qrow, qvalue.QValueInt32{Val: *v})
			default:
				return nil, fmt.Errorf("cannot convert %T to qvalue", v)
			}
		}
		batch.Records = append(batch.Records, qrow)
	}

	return batch, rows.Err()
}

func SetupSuite(t *testing.T) ClickHouseSuite {
	t.Helper()

	suffix := "ch_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	s3Helper, err := e2e_s3.NewS3TestHelper(false)
	require.NoError(t, err, "failed to setup S3")

	s := ClickHouseSuite{
		t:        t,
		conn:     conn,
		suffix:   suffix,
		s3Helper: s3Helper,
	}

	ch, err := connclickhouse.Connect(context.Background(), s.PeerForDatabase("default").GetClickhouseConfig())
	require.NoError(t, err, "failed to connect to clickhouse")
	err = ch.Exec(context.Background(), "CREATE DATABASE e2e_test_"+suffix)
	require.NoError(t, err, "failed to create clickhouse database")

	return s
}
