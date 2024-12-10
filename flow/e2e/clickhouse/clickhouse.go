package e2e_clickhouse

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	chgo "github.com/ClickHouse/ch-go"
	chproto "github.com/ClickHouse/ch-go/proto"
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

// from clickhouse-go lib/column/bigint.go
func rawToBigInt(v []byte, signed bool) *big.Int {
	// LittleEndian to BigEndian
	slices.Reverse(v)
	lt := new(big.Int)
	if signed && len(v) > 0 && v[0]&0x80 != 0 {
		// [0] ^ will +1
		for i := 0; i < len(v); i++ {
			v[i] = ^v[i]
		}
		lt.SetBytes(v)
		// neg ^ will -1
		lt.Not(lt)
	} else {
		lt.SetBytes(v)
	}
	return lt
}

// largely taken from clickhouse-go lib/column/decimal.go
func decimalRow(col chproto.ColResult, i int) decimal.Decimal {
	typ := string(col.Type())
	lparam := strings.LastIndex(typ, "(")
	scale := 0
	if lparam != -1 {
		params := typ[lparam+1:]
		rparam := strings.Index(params, ")")
		if rparam == -1 {
			panic("no ) in params " + params + " of " + typ)
		}
		params = typ[:rparam]
		_, scaleStr, ok := strings.Cut(params, ",")
		if !ok {
			panic("no , in params " + params + " of " + typ)
		}
		scaleStr = strings.TrimSpace(scaleStr)
		var err error
		scale, err = strconv.Atoi(scaleStr)
		if err != nil {
			panic("failed to parse scale " + scaleStr + ": " + err.Error())
		}
	}

	var value decimal.Decimal
	switch vCol := col.(type) {
	case *chproto.ColDecimal32:
		v := vCol.Row(i)
		value = decimal.New(int64(v), int32(-scale))
	case *chproto.ColDecimal64:
		v := vCol.Row(i)
		value = decimal.New(int64(v), int32(-scale))
	case *chproto.ColDecimal128:
		v := vCol.Row(i)
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:64/8], v.Low)
		binary.LittleEndian.PutUint64(b[64/8:128/8], v.High)
		bv := rawToBigInt(b, true)
		value = decimal.NewFromBigInt(bv, int32(-scale))
	case *chproto.ColDecimal256:
		v := vCol.Row(i)
		b := make([]byte, 32)
		binary.LittleEndian.PutUint64(b[0:64/8], v.Low.Low)
		binary.LittleEndian.PutUint64(b[64/8:128/8], v.Low.High)
		binary.LittleEndian.PutUint64(b[128/8:192/8], v.High.Low)
		binary.LittleEndian.PutUint64(b[192/8:256/8], v.High.High)
		bv := rawToBigInt(b, true)
		value = decimal.NewFromBigInt(bv, int32(-scale))
	}
	return value
}

func (s ClickHouseSuite) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	ch, err := connclickhouse.Connect(context.Background(), nil, s.Peer().GetClickhouseConfig())
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	batch := &model.QRecordBatch{}
	var schema chproto.Results
	if err := ch.Do(context.Background(), chgo.Query{
		Body:   fmt.Sprintf(`SELECT %s FROM %s FINAL WHERE _peerdb_is_deleted = 0 ORDER BY 1 SETTINGS use_query_cache = false`, cols, table),
		Result: schema.Auto(),
		OnResult: func(ctx context.Context, block chproto.Block) error {
			if len(batch.Schema.Fields) == 0 {
				tableSchema, err := connclickhouse.GetTableSchemaForTable(table, schema)
				if err != nil {
					return err
				}
				for _, col := range tableSchema.Columns {
					batch.Schema.Fields = append(batch.Schema.Fields, qvalue.QField{
						Name:      col.Name,
						Type:      qvalue.QValueKind(col.Type),
						Precision: 0,
						Scale:     0,
						Nullable:  col.Nullable,
					})
				}
			}

			for idx := range block.Rows {
				qrow := make([]qvalue.QValue, 0, block.Columns)
				for _, col := range schema {
					switch v := col.Data.(type) {
					case *chproto.ColNullable[string]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindString))
						} else {
							qrow = append(qrow, qvalue.QValueString{Val: v.Values.Row(idx)})
						}
					case *chproto.ColStr:
						qrow = append(qrow, qvalue.QValueString{Val: v.Row(idx)})
					case *chproto.ColNullable[int16]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt32))
						} else {
							qrow = append(qrow, qvalue.QValueInt16{Val: v.Values.Row(idx)})
						}
					case *chproto.ColInt16:
						qrow = append(qrow, qvalue.QValueInt16{Val: v.Row(idx)})
					case *chproto.ColNullable[int32]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt32))
						} else {
							qrow = append(qrow, qvalue.QValueInt32{Val: v.Values.Row(idx)})
						}
					case *chproto.ColInt32:
						qrow = append(qrow, qvalue.QValueInt32{Val: v.Row(idx)})
					case *chproto.ColNullable[int64]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindInt32))
						} else {
							qrow = append(qrow, qvalue.QValueInt64{Val: v.Values.Row(idx)})
						}
					case *chproto.ColInt64:
						qrow = append(qrow, qvalue.QValueInt64{Val: v.Row(idx)})
					case *chproto.ColNullable[time.Time]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindTimestamp))
						} else {
							qrow = append(qrow, qvalue.QValueTimestamp{Val: v.Values.Row(idx)})
						}
					case *chproto.ColDate32:
						qrow = append(qrow, qvalue.QValueTimestamp{Val: v.Row(idx)})
					case *chproto.ColDateTime64:
						qrow = append(qrow, qvalue.QValueTimestamp{Val: v.Row(idx)})
					case *chproto.ColNullable[chproto.Decimal32]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindNumeric))
						} else {
							qrow = append(qrow, qvalue.QValueNumeric{Val: decimalRow(v.Values, idx)})
						}
					case *chproto.ColNullable[chproto.Decimal64]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindNumeric))
						} else {
							qrow = append(qrow, qvalue.QValueNumeric{Val: decimalRow(v.Values, idx)})
						}
					case *chproto.ColNullable[chproto.Decimal128]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindNumeric))
						} else {
							qrow = append(qrow, qvalue.QValueNumeric{Val: decimalRow(v.Values, idx)})
						}
					case *chproto.ColNullable[chproto.Decimal256]:
						if v.Nulls[idx] != 0 {
							qrow = append(qrow, qvalue.QValueNull(qvalue.QValueKindNumeric))
						} else {
							qrow = append(qrow, qvalue.QValueNumeric{Val: decimalRow(v.Values, idx)})
						}
					case *chproto.ColDecimal32,
						*chproto.ColDecimal64,
						*chproto.ColDecimal128,
						*chproto.ColDecimal256:
						qrow = append(qrow, qvalue.QValueNumeric{Val: decimalRow(v, idx)})
					default:
						return fmt.Errorf("cannot convert %T to qvalue", v)
					}
				}
				batch.Records = append(batch.Records, qrow)
			}

			return nil
		},
	}); err != nil {
		return nil, err
	}

	return batch, nil
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
	err = ch.Do(context.Background(), chgo.Query{Body: "CREATE DATABASE e2e_test_" + suffix})
	require.NoError(t, err, "failed to create clickhouse database")

	return s
}
