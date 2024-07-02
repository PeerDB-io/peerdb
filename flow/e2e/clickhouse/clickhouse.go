package e2e_clickhouse

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2e/s3"
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
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "click"),
		Type: protos.DBType_CLICKHOUSE,
		Config: &protos.Peer_ClickhouseConfig{
			ClickhouseConfig: &protos.ClickhouseConfig{
				Host:            "localhost",
				Port:            9000,
				Database:        "default",
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
	e2e.TearDownPostgres(s)
}

func (s ClickHouseSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	ch, err := connclickhouse.Connect(context.Background(), s.Peer().GetClickhouseConfig())
	if err != nil {
		return nil, err
	}

	rows, err := ch.Query(
		fmt.Sprintf(`SELECT %s FROM e2e_test_%s.%s ORDER BY id`, cols, s.suffix, table),
	)
	if err != nil {
		return nil, err
	}

	batch := &model.QRecordBatch{}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	for _, ty := range types {
		prec, scale, _ := ty.DecimalSize()
		nullable, _ := ty.Nullable()
		batch.Schema.Fields = append(batch.Schema.Fields, qvalue.QField{
			Name:      ty.Name(),
			Type:      qvalue.QValueKind(ty.DatabaseTypeName()), // TODO do the right thing
			Precision: int16(prec),
			Scale:     int16(scale),
			Nullable:  nullable,
		})
	}

	for rows.Next() {
		// TODO rows.Scan()
		// TODO batch.Records = append(batch.Records, record)
	}

	return batch, rows.Err()
}

func SetupSuite(t *testing.T) ClickHouseSuite {
	t.Helper()

	suffix := "ch_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	s3Helper, err := e2e_s3.NewS3TestHelper(false)
	if err != nil {
		require.Fail(t, "failed to setup S3", err)
	}

	return ClickHouseSuite{
		t:        t,
		conn:     conn,
		suffix:   suffix,
		s3Helper: s3Helper,
	}
}
