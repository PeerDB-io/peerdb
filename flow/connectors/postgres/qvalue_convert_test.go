package connpostgres

import (
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/clickhouse"
)

// TODO: turn this test into an integration test and use TestContainer to query
// an actual postgres database to fetch an exhaustive list of all pg types, to
// avoid explicitly listing all types.
var pgTypeInfo = []uint32{
	pgtype.BoolOID,
	pgtype.ByteaOID,
	pgtype.QCharOID,
	pgtype.NameOID,
	pgtype.Int8OID,
	pgtype.Int2OID,
	pgtype.Int4OID,
	pgtype.TextOID,
	pgtype.OIDOID,
	pgtype.TIDOID,
	pgtype.XIDOID,
	pgtype.CIDOID,
	pgtype.JSONOID,
	pgtype.XMLOID,
	pgtype.XMLArrayOID,
	pgtype.JSONArrayOID,
	pgtype.XID8ArrayOID,
	pgtype.PointOID,
	pgtype.LsegOID,
	pgtype.PathOID,
	pgtype.BoxOID,
	pgtype.PolygonOID,
	pgtype.LineOID,
	pgtype.LineArrayOID,
	pgtype.CIDROID,
	pgtype.CIDRArrayOID,
	pgtype.Float4OID,
	pgtype.Float8OID,
	pgtype.CircleOID,
	pgtype.CircleArrayOID,
	pgtype.UnknownOID,
	pgtype.Macaddr8OID,
	pgtype.MacaddrOID,
	pgtype.InetOID,
	pgtype.BoolArrayOID,
	pgtype.QCharArrayOID,
	pgtype.NameArrayOID,
	pgtype.Int2ArrayOID,
	pgtype.Int4ArrayOID,
	pgtype.TextArrayOID,
	pgtype.TIDArrayOID,
	pgtype.ByteaArrayOID,
	pgtype.XIDArrayOID,
	pgtype.CIDArrayOID,
	pgtype.BPCharArrayOID,
	pgtype.VarcharArrayOID,
	pgtype.Int8ArrayOID,
	pgtype.PointArrayOID,
	pgtype.LsegArrayOID,
	pgtype.PathArrayOID,
	pgtype.BoxArrayOID,
	pgtype.Float4ArrayOID,
	pgtype.Float8ArrayOID,
	pgtype.PolygonArrayOID,
	pgtype.OIDArrayOID,
	pgtype.ACLItemOID,
	pgtype.ACLItemArrayOID,
	pgtype.MacaddrArrayOID,
	pgtype.InetArrayOID,
	pgtype.BPCharOID,
	pgtype.VarcharOID,
	pgtype.DateOID,
	pgtype.TimeOID,
	pgtype.TimestampOID,
	pgtype.TimestampArrayOID,
	pgtype.DateArrayOID,
	pgtype.TimeArrayOID,
	pgtype.TimestamptzOID,
	pgtype.TimestamptzArrayOID,
	pgtype.IntervalOID,
	pgtype.IntervalArrayOID,
	pgtype.NumericArrayOID,
	pgtype.TimetzOID,
	pgtype.BitOID,
	pgtype.BitArrayOID,
	pgtype.VarbitOID,
	pgtype.VarbitArrayOID,
	pgtype.NumericOID,
	pgtype.RecordOID,
	pgtype.RecordArrayOID,
	pgtype.UUIDOID,
	pgtype.UUIDArrayOID,
	pgtype.JSONBOID,
	pgtype.JSONBArrayOID,
	pgtype.DaterangeOID,
	pgtype.DaterangeArrayOID,
	pgtype.Int4rangeOID,
	pgtype.Int4rangeArrayOID,
	pgtype.NumrangeOID,
	pgtype.NumrangeArrayOID,
	pgtype.TsrangeOID,
	pgtype.TsrangeArrayOID,
	pgtype.TstzrangeOID,
	pgtype.TstzrangeArrayOID,
	pgtype.Int8rangeOID,
	pgtype.Int8rangeArrayOID,
	pgtype.JSONPathOID,
	pgtype.JSONPathArrayOID,
	pgtype.Int4multirangeOID,
	pgtype.NummultirangeOID,
	pgtype.TsmultirangeOID,
	pgtype.TstzmultirangeOID,
	pgtype.DatemultirangeOID,
	pgtype.Int8multirangeOID,
	pgtype.XID8OID,
	// pgtype.TimetzArrayOID,          // not supported
	// pgtype.Int4multirangeArrayOID,  // not supported
	// pgtype.NummultirangeArrayOID,   // not supported
	// pgtype.TsmultirangeArrayOID,    // not supported
	// pgtype.TstzmultirangeArrayOID,  // not supported
	// pgtype.DatemultirangeArrayOID,  // not supported
	// pgtype.Int8multirangeArrayOID,  // not supported
}

// TODO: add test cases for:
//  1. type modifiers != -1
//  2. custom types
//  3. nullable=true
func TestPostgresOidToClickHouseType(t *testing.T) {
	ctx := t.Context()
	logger := internal.LoggerFromCtx(ctx)
	newMap := pgtype.NewMap()
	hushWarnOID := make(map[uint32]struct{})
	customTypeMap := map[uint32]shared.CustomDataType{}
	env := map[string]string{
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
	}
	nullable := false
	nullableEnabled := false
	for _, oid := range pgTypeInfo {
		pgType, err := postgresOidToName(oid, customTypeMap, newMap)
		require.NoError(t, err)

		fd := &protos.FieldDescription{
			Name:         strconv.FormatUint(uint64(oid), 10),
			TypeModifier: -1,
			Nullable:     nullable,
			Type:         pgType,
		}

		kind := postgresOIDToQValueKind(oid, customTypeMap, newMap, hushWarnOID, logger)
		derivedPgToChType, err := kind.ToDWHColumnType(
			ctx,
			env,
			protos.DBType_CLICKHOUSE,
			fd,
			nullableEnabled,
		)
		require.NoError(t, err)
		directPgToChType := clickhouse.PostgresToClickHouseType(oid, fd.TypeModifier, fd.Nullable)

		require.Equal(t, derivedPgToChType, directPgToChType, "Mismatch type for oid=%d", oid)
	}

}
