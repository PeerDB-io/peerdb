package e2e_clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	e2e_s3 "github.com/PeerDB-io/peerdb/flow/e2e/s3"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

type ClickHouseSuite struct {
	t         *testing.T
	source    e2e.SuiteSource
	s3Helper  *e2e_s3.S3TestHelper
	connector *connclickhouse.ClickHouseConnector
	suffix    string
}

func (s ClickHouseSuite) T() *testing.T {
	return s.t
}

func (s ClickHouseSuite) Connector() *connpostgres.PostgresConnector {
	c, ok := s.source.Connector().(*connpostgres.PostgresConnector)
	if !ok {
		s.t.Skipf("skipping test because it relies on PostgresConnector, while source is %T", s.source)
	}
	return c
}

func (s ClickHouseSuite) Source() e2e.SuiteSource {
	return s.source
}

func (s ClickHouseSuite) DestinationConnector() connectors.Connector {
	return s.connector
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

func (s ClickHouseSuite) Teardown(ctx context.Context) {
	require.NoError(s.t, s.s3Helper.CleanUp(ctx))
	s.source.Teardown(s.t, ctx, s.Suffix())
	require.NoError(s.t, s.connector.Close())
}

type TestClickHouseColumn struct {
	Name string
	Type string
}

// CreateRMTTable creates a ReplacingMergeTree table with the given name and columns.
func (s ClickHouseSuite) CreateRMTTable(tableName string, columns []TestClickHouseColumn, orderingKey string) error {
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	if err != nil {
		return err
	}
	defer ch.Close()

	columnStrings := make([]string, len(columns))
	for i, col := range columns {
		columnStrings[i] = fmt.Sprintf("`%s` %s", col.Name, col.Type)
	}

	// Join the column definitions into a single string
	columnStr := strings.Join(columnStrings, ", ")

	// Create the table with ReplacingMergeTree engine
	createTableQuery := fmt.Sprintf("CREATE TABLE `%s` (%s) ENGINE = ReplacingMergeTree() ORDER BY `%s`", tableName, columnStr, orderingKey)
	return ch.Exec(s.t.Context(), createTableQuery)
}

func (s ClickHouseSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	rows, err := ch.Query(
		s.t.Context(),
		fmt.Sprintf(`SELECT %s FROM %s FINAL WHERE _peerdb_is_deleted = 0 ORDER BY 1 SETTINGS use_query_cache = false`, cols, table),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	batch := &model.QRecordBatch{}
	types := rows.ColumnTypes()
	row := make([]any, 0, len(types))
	tableSchema, err := connclickhouse.GetTableSchemaForTable(&protos.TableMapping{SourceTableIdentifier: table}, types)
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
			case *[]string:
				qrow = append(qrow, qvalue.QValueArrayString{Val: *v})
			case **int8:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt8))
				} else {
					qrow = append(qrow, qvalue.QValueInt8{Val: **v})
				}
			case *int8:
				qrow = append(qrow, qvalue.QValueInt8{Val: *v})
			case **int16:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt16))
				} else {
					qrow = append(qrow, qvalue.QValueInt16{Val: **v})
				}
			case *int16:
				qrow = append(qrow, qvalue.QValueInt16{Val: *v})
			case **int32:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt32))
				} else {
					qrow = append(qrow, qvalue.QValueInt32{Val: **v})
				}
			case *int32:
				qrow = append(qrow, qvalue.QValueInt32{Val: *v})
			case *[]int32:
				qrow = append(qrow, qvalue.QValueArrayInt32{Val: *v})
			case **int64:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt64))
				} else {
					qrow = append(qrow, qvalue.QValueInt64{Val: **v})
				}
			case *int64:
				qrow = append(qrow, qvalue.QValueInt64{Val: *v})
			case **time.Time:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindTimestamp))
				} else {
					qrow = append(qrow, qvalue.QValueTimestamp{Val: **v})
				}
			case **uint8:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindUInt8))
				} else {
					qrow = append(qrow, qvalue.QValueUInt8{Val: **v})
				}
			case *uint8:
				qrow = append(qrow, qvalue.QValueUInt8{Val: *v})
			case **uint16:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindUInt16))
				} else {
					qrow = append(qrow, qvalue.QValueUInt16{Val: **v})
				}
			case *uint16:
				qrow = append(qrow, qvalue.QValueUInt16{Val: *v})
			case **uint32:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindUInt32))
				} else {
					qrow = append(qrow, qvalue.QValueUInt32{Val: **v})
				}
			case *uint32:
				qrow = append(qrow, qvalue.QValueUInt32{Val: *v})
			case **uint64:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindUInt64))
				} else {
					qrow = append(qrow, qvalue.QValueUInt64{Val: **v})
				}
			case *uint64:
				qrow = append(qrow, qvalue.QValueUInt64{Val: *v})
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
			case *[]float32:
				qrow = append(qrow, qvalue.QValueArrayFloat32{Val: *v})
			case **float64:
				if *v == nil {
					qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindFloat64))
				} else {
					qrow = append(qrow, qvalue.QValueFloat64{Val: **v})
				}
			case *float64:
				qrow = append(qrow, qvalue.QValueFloat64{Val: *v})
			case *[]float64:
				qrow = append(qrow, qvalue.QValueArrayFloat64{Val: *v})
			case *uuid.UUID:
				qrow = append(qrow, qvalue.QValueUUID{Val: *v})
			case *[]uuid.UUID:
				qrow = append(qrow, qvalue.QValueArrayUUID{Val: *v})
			default:
				return nil, fmt.Errorf("cannot convert %T to qvalue", v)
			}
		}
		batch.Records = append(batch.Records, qrow)
	}

	return batch, rows.Err()
}

func SetupSuite[TSource e2e.SuiteSource](
	t *testing.T,
	setupSource func(*testing.T) (TSource, string, error),
) func(*testing.T) ClickHouseSuite {
	t.Helper()
	return func(t *testing.T) ClickHouseSuite {
		t.Helper()

		source, suffix, err := setupSource(t)
		require.NoError(t, err, "failed to setup postgres")

		s3Helper, err := e2e_s3.NewS3TestHelper(t.Context(), e2e_s3.Minio)
		require.NoError(t, err, "failed to setup S3")

		s := ClickHouseSuite{
			t:        t,
			source:   e2e.SuiteSource(source),
			suffix:   suffix,
			s3Helper: s3Helper,
		}

		ch, err := connclickhouse.Connect(t.Context(), nil, s.PeerForDatabase("default").GetClickhouseConfig())
		require.NoError(t, err, "failed to connect to clickhouse")
		err = ch.Exec(t.Context(), "CREATE DATABASE e2e_test_"+suffix)
		require.NoError(t, err, "failed to create clickhouse database")

		connector, err := connclickhouse.NewClickHouseConnector(t.Context(), nil, s.Peer().GetClickhouseConfig())
		require.NoError(t, err)
		s.connector = connector

		return s
	}
}
