package e2e_clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
	"github.com/PeerDB-io/peerdb/flow/shared/types"
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
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, dbname),
		Type: protos.DBType_CLICKHOUSE,
		Config: &protos.Peer_ClickhouseConfig{
			ClickhouseConfig: &protos.ClickhouseConfig{
				Host:       "localhost",
				Port:       9000,
				Database:   dbname,
				DisableTls: true,
				S3:         s.s3Helper.S3Config,
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

	var rows driver.Rows
	var rowsErr error
	if strings.HasPrefix(table, "_peerdb_raw") {
		rows, rowsErr = s.queryRawTable(ch, table, cols)
	} else {
		rows, rowsErr = ch.Query(
			s.t.Context(),
			fmt.Sprintf(`SELECT %s FROM %s FINAL WHERE _peerdb_is_deleted = 0 ORDER BY 1 SETTINGS use_query_cache = false`, cols, table),
		)
	}
	if rowsErr != nil {
		return nil, rowsErr
	}
	defer rows.Close()

	batch := &model.QRecordBatch{}
	colTypes := rows.ColumnTypes()
	row := make([]any, 0, len(colTypes))
	tableSchema, err := connclickhouse.GetTableSchemaForTable(&protos.TableMapping{SourceTableIdentifier: table}, colTypes)
	if err != nil {
		return nil, err
	}

	for idx, ty := range colTypes {
		fieldDesc := tableSchema.Columns[idx]
		row = append(row, reflect.New(ty.ScanType()).Interface())
		batch.Schema.Fields = append(batch.Schema.Fields, types.QField{
			Name:      ty.Name(),
			Type:      types.QValueKind(fieldDesc.Type),
			Precision: 0,
			Scale:     0,
			Nullable:  fieldDesc.Nullable,
		})
	}

	for rows.Next() {
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		qrow := make([]types.QValue, 0, len(row))
		for _, val := range row {
			switch v := val.(type) {
			case **string:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindString))
				} else {
					qrow = append(qrow, types.QValueString{Val: **v})
				}
			case *string:
				qrow = append(qrow, types.QValueString{Val: *v})
			case *[]string:
				qrow = append(qrow, types.QValueArrayString{Val: *v})
			case **int8:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindInt8))
				} else {
					qrow = append(qrow, types.QValueInt8{Val: **v})
				}
			case *int8:
				qrow = append(qrow, types.QValueInt8{Val: *v})
			case **int16:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindInt16))
				} else {
					qrow = append(qrow, types.QValueInt16{Val: **v})
				}
			case *int16:
				qrow = append(qrow, types.QValueInt16{Val: *v})
			case **int32:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindInt32))
				} else {
					qrow = append(qrow, types.QValueInt32{Val: **v})
				}
			case *int32:
				qrow = append(qrow, types.QValueInt32{Val: *v})
			case *[]int32:
				qrow = append(qrow, types.QValueArrayInt32{Val: *v})
			case **int64:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindInt64))
				} else {
					qrow = append(qrow, types.QValueInt64{Val: **v})
				}
			case *int64:
				qrow = append(qrow, types.QValueInt64{Val: *v})
			case **time.Time:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindTimestamp))
				} else {
					qrow = append(qrow, types.QValueTimestamp{Val: **v})
				}
			case **uint8:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindUInt8))
				} else {
					qrow = append(qrow, types.QValueUInt8{Val: **v})
				}
			case *uint8:
				qrow = append(qrow, types.QValueUInt8{Val: *v})
			case **uint16:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindUInt16))
				} else {
					qrow = append(qrow, types.QValueUInt16{Val: **v})
				}
			case *uint16:
				qrow = append(qrow, types.QValueUInt16{Val: *v})
			case **uint32:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindUInt32))
				} else {
					qrow = append(qrow, types.QValueUInt32{Val: **v})
				}
			case *uint32:
				qrow = append(qrow, types.QValueUInt32{Val: *v})
			case **uint64:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindUInt64))
				} else {
					qrow = append(qrow, types.QValueUInt64{Val: **v})
				}
			case *uint64:
				qrow = append(qrow, types.QValueUInt64{Val: *v})
			case *time.Time:
				qrow = append(qrow, types.QValueTimestamp{Val: *v})
			case *[]time.Time:
				qrow = append(qrow, types.QValueArrayTimestamp{Val: *v})
			case **decimal.Decimal:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindNumeric))
				} else {
					qrow = append(qrow, types.QValueNumeric{Val: **v})
				}
			case *decimal.Decimal:
				qrow = append(qrow, types.QValueNumeric{Val: *v})
			case *[]decimal.Decimal:
				qrow = append(qrow, types.QValueArrayNumeric{Val: *v})
			case **bool:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindBoolean))
				} else {
					qrow = append(qrow, types.QValueBoolean{Val: **v})
				}
			case *bool:
				qrow = append(qrow, types.QValueBoolean{Val: *v})
			case **float32:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindFloat32))
				} else {
					qrow = append(qrow, types.QValueFloat32{Val: **v})
				}
			case *float32:
				qrow = append(qrow, types.QValueFloat32{Val: *v})
			case *[]float32:
				qrow = append(qrow, types.QValueArrayFloat32{Val: *v})
			case **float64:
				if *v == nil {
					qrow = append(qrow, types.QValueNull(types.QValueKindFloat64))
				} else {
					qrow = append(qrow, types.QValueFloat64{Val: **v})
				}
			case *float64:
				qrow = append(qrow, types.QValueFloat64{Val: *v})
			case *[]float64:
				qrow = append(qrow, types.QValueArrayFloat64{Val: *v})
			case *uuid.UUID:
				qrow = append(qrow, types.QValueUUID{Val: *v})
			case *[]uuid.UUID:
				qrow = append(qrow, types.QValueArrayUUID{Val: *v})
			default:
				return nil, fmt.Errorf("cannot convert %T to types", v)
			}
		}
		batch.Records = append(batch.Records, qrow)
	}

	return batch, rows.Err()
}

func (s ClickHouseSuite) queryRawTable(conn clickhouse.Conn, table string, cols string) (driver.Rows, error) {
	return conn.Query(
		s.t.Context(),
		fmt.Sprintf(`SELECT %s FROM %s ORDER BY 1 SETTINGS use_query_cache = false`, cols, table),
	)
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
