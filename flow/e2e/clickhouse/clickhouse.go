package e2e_clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
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
	region := ""
	if s.s3Helper.S3Config.Region != nil {
		region = *s.s3Helper.S3Config.Region
	}

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
				Region:          region,
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
	ch, err := connclickhouse.Connect(context.Background(), nil, s.Peer().GetClickhouseConfig())
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	rows, err := ch.Query(
		context.Background(),
		fmt.Sprintf(`SELECT %s FROM %s FINAL WHERE _peerdb_is_deleted = 0 ORDER BY 1 SETTINGS use_query_cache = false`, cols, table),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	batch := &model.QRecordBatch{}
	types := rows.ColumnTypes()
	row := make([]interface{}, 0, len(types))
	tableSchema, err := connclickhouse.GetTableSchemaForTable(table, types)
	if err != nil {
		return nil, err
	}

	for idx, ty := range types {
		fieldDesc := tableSchema.Columns[idx]
		row = append(row, reflect.New(ty.ScanType()).Interface())
		batch.Schema.Fields = append(batch.Schema.Fields, qvalue.QField{
			Name:      ty.Name(),
			Type:      qvalue.QValueKind(fieldDesc.Type),
			Precision: 0,
			Scale:     0,
			Nullable:  fieldDesc.Nullable,
		})
	}

	for rows.Next() {
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		qrow := make([]qvalue.QValue, 0, len(row))
		for _, val := range row {
			switch v := val.(type) {
			case **string:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindString))
				} else {
					qrow = append(qrow, qvalue.QValueString{Val: **v})
				}
			case *string:
				qrow = append(qrow, qvalue.QValueString{Val: *v})
			case **int32:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt32))
				} else {
					qrow = append(qrow, qvalue.QValueInt32{Val: **v})
				}
			case *int32:
				qrow = append(qrow, qvalue.QValueInt32{Val: *v})
			case **time.Time:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindTimestamp))
				} else {
					qrow = append(qrow, qvalue.QValueTimestamp{Val: **v})
				}
			case *time.Time:
				qrow = append(qrow, qvalue.QValueTimestamp{Val: *v})
			case **decimal.Decimal:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindNumeric))
				} else {
					qrow = append(qrow, qvalue.QValueNumeric{Val: **v})
				}
			case *decimal.Decimal:
				qrow = append(qrow, qvalue.QValueNumeric{Val: *v})
			case **bool:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindBoolean))
				} else {
					qrow = append(qrow, qvalue.QValueBoolean{Val: **v})
				}
			case *bool:
				qrow = append(qrow, qvalue.QValueBoolean{Val: *v})
			case **float32:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindFloat32))
				} else {
					qrow = append(qrow, qvalue.QValueFloat32{Val: **v})
				}
			case *float32:
				qrow = append(qrow, qvalue.QValueFloat32{Val: *v})
			case **float64:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindFloat64))
				} else {
					qrow = append(qrow, qvalue.QValueFloat64{Val: **v})
				}
			case *float64:
				qrow = append(qrow, qvalue.QValueFloat64{Val: *v})
			case **int64:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt64))
				} else {
					qrow = append(qrow, qvalue.QValueInt64{Val: **v})
				}
			case *int64:
				qrow = append(qrow, qvalue.QValueInt64{Val: *v})
			case **int16:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt16))
				} else {
					qrow = append(qrow, qvalue.QValueInt16{Val: **v})
				}
			case *int16:
				qrow = append(qrow, qvalue.QValueInt16{Val: *v})
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

	s3Helper, err := e2e_s3.NewS3TestHelper(e2e_s3.Minio)
	require.NoError(t, err, "failed to setup S3")

	s := ClickHouseSuite{
		t:        t,
		conn:     conn,
		suffix:   suffix,
		s3Helper: s3Helper,
	}

	ch, err := connclickhouse.Connect(context.Background(), nil, s.PeerForDatabase("default").GetClickhouseConfig())
	require.NoError(t, err, "failed to connect to clickhouse")
	err = ch.Exec(context.Background(), "CREATE DATABASE e2e_test_"+suffix)
	require.NoError(t, err, "failed to create clickhouse database")

	return s
}
