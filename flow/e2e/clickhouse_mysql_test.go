package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func mysqlEnumUsesOrdinals(source *MySqlSource) bool {
	return source.Config.Flavor == protos.MySqlFlavor_MYSQL_MYSQL &&
		source.Config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_FILEPOS
}

func (s ClickHouseSuite) Test_UnsignedMySQL() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_unsigned"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_unsigned"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`CREATE TABLE %s (
		id serial primary key,
		i8 tinyint, u8 tinyint unsigned,
		i16 smallint, u16 smallint unsigned,
		i24 mediumint zerofill, u24 mediumint unsigned,
		i32 int, u32 int unsigned zerofill,
		i64 bigint, u64 bigint unsigned,
		d decimal(7, 6), b boolean
	)`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`insert into %s
		(i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b)
		values (-1, 200, -2, 40000, -3, 10000000, -4, 3000000000, %d, %d, 3.141592,true)
	`, srcFullName, int64(math.MinInt64), uint64(math.MaxUint64))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      srcFullName,
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`insert into %s
		(i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b)
		values (-1, 200, -2, 40000, -3, 10000000, -4, 3000000000, %d, %d, 3.141592,false)
	`, srcFullName, int64(math.MinInt64), uint64(math.MaxUint64))))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,i8,u8,i16,u16,i24,u24,i32,u32,i64,u64,d,b")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Time() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_datetime"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_datetime_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			d DATE NOT NULL,
			dt DATETIME NOT NULL,
			tm TIMESTAMP(6) NOT NULL,
			t TIME NOT NULL,
			y YEAR NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",d,dt,tm,t,y) VALUES
		('init','1935-01-01','1953-02-02 12:01:02','1973-02-02 13:01:02.123','14:21.654321',1935),
		('init','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','00:00',0),
		('init','2000-01-00','2000-00-01 00:00:00','2000-01-01 00:00:00.000','-800:0:1',2155)`,
		quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",d,dt,tm,t,y")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",d,dt,tm,t,y) VALUES
		('cdc','1935-01-01','1953-02-02 12:01:02','1973-02-02 13:01:02.123','14:21.654321',1935),
		('cdc','0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00.000','00:00',0),
		('cdc','2000-01-00','2000-00-01 00:00:00','2000-01-01 00:00:00.000','-800:0:1',2155)`,
		quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",d,dt,tm,t,y")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Bit() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_bit"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_bit_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			b1 bit(1) NOT NULL,
			b20 bit(20) NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",b1,b20) VALUES
		('init',b'1',b'11100011100011100011'), ('init',b'0',b'00011100011100011100')`, quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",b1,b20")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",b1,b20) VALUES
		('cdc','1','11100011100011100011'), ('cdc','0','00011100011100011100')`, quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",b1,b20")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Blobs() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_blobs"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_blobs_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			k TEXT NOT NULL,
			tb tinyblob NOT NULL,
			mb mediumblob NOT NULL,
			lb longblob NOT NULL,
			bi binary(6) NOT NULL,
			vb varbinary(100) NOT NULL,
			tt tinytext NOT NULL,
			mt mediumtext NOT NULL,
			lt longtext NOT NULL,
			ch char(4) NOT NULL,
			vc varchar(100) NOT NULL,
			js json NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js) VALUES
		('init','tinyblob','mediumblob','longblob','binary','varbinary',
		'tinytext','mediumtext','longtext','char','varchar','{"a":2}')`, quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,k,tb,mb,lb,vb,bi,tt,mt,lt,ch,vc,js")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s (k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js) VALUES
		('cdc','tinyblob','mediumblob','longblob','binary','varbinary',
		'tinytext','mediumtext','longtext','char','varchar','{"a":2}')`, quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,k,tb,mb,lb,bi,vb,tt,mt,lt,ch,vc,js")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_TransactionPayloadCompression() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}
	if mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("binlog_transaction_compression is not supported by MariaDB")
	}
	cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogTransactionCompression)
	require.NoError(s.t, err)
	if cmp < 0 {
		s.t.Skip("only applies to mysql versions with binlog_transaction_compression")
	}

	srcTableName := "test_txn_payload"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_txn_payload_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT PRIMARY KEY,
			val TEXT NOT NULL
		)
	`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Compress this session's transactions so the upcoming DML lands in the binlog as a single
	// TRANSACTION_PAYLOAD_EVENT, exercising the compressed-payload decode path in CDC.
	require.NoError(s.t, s.source.Exec(s.t.Context(), "SET SESSION binlog_transaction_compression = ON"))

	// Capture the binlog position, then write a large, highly compressible transaction as a single
	// multi-row INSERT so MySQL compresses it (compressed < uncompressed) into one payload event.
	posBefore, err := mySource.GetMasterPos(s.t.Context())
	require.NoError(s.t, err)

	const rowCount = 200
	rowVal := strings.Repeat("peerdb-compressible-", 100) // ~2KB highly repetitive per row
	var insert strings.Builder
	fmt.Fprintf(&insert, "INSERT INTO %s (id, val) VALUES ", srcFullName)
	for i := 1; i <= rowCount; i++ {
		if i > 1 {
			insert.WriteByte(',')
		}
		fmt.Fprintf(&insert, "(%d, '%s')", i, rowVal)
	}
	require.NoError(s.t, s.source.Exec(s.t.Context(), insert.String()))

	findPayloadEndPos := func(ctx context.Context, from mysql.Position) (bool, uint32, error) {
		rs, err := mySource.Execute(ctx, fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d", from.Name, from.Pos))
		if err != nil {
			return false, 0, err
		}
		for i := range rs.Values {
			// SHOW BINLOG EVENTS columns: Log_name, Pos, Event_type, Server_id, End_log_pos, Info
			eventType, _ := rs.GetString(i, 2)
			if strings.Contains(strings.ToLower(eventType), "payload") {
				pos, _ := rs.GetInt(i, 4)
				return true, uint32(pos), nil
			}
		}
		return false, 0, nil
	}
	hasPayload, payloadEndPos, err := findPayloadEndPos(s.t.Context(), posBefore)
	require.NoError(s.t, err)
	require.True(s.t, hasPayload, "expected a TRANSACTION_PAYLOAD_EVENT in the binlog")

	// for GTID "SHOW BINLOG EVENTS" returns really complex format and it's different
	// across versions/flavors.
	// Therefore, we just get the latest GTID from a server instead of reading it from a binglog.
	gtidAfter, err := mySource.GetMasterGTIDSet(s.t.Context())
	require.NoError(s.t, err)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,val")

	// Assert the payload transaction was checkpointed
	pool, err := catalogTestAccessPool()
	require.NoError(s.t, err)
	var lastText string
	require.NoError(s.t, pool.QueryRow(s.t.Context(),
		"SELECT last_text FROM metadata_last_sync_state WHERE job_name = $1",
		flowConnConfig.FlowJobName,
	).Scan(&lastText))

	if rest, isFilePos := strings.CutPrefix(lastText, "!f:"); isFilePos {
		comma := strings.LastIndexByte(rest, ',')
		require.NotEqual(s.t, -1, comma, "malformed file/pos offset %q", lastText)
		storedPos, parseErr := strconv.ParseUint(rest[comma+1:], 16, 32)
		require.NoError(s.t, parseErr)
		require.GreaterOrEqual(s.t, uint32(storedPos), payloadEndPos,
			"checkpoint did not advance past the TRANSACTION_PAYLOAD_EVENT")
	} else {
		storedSet, parseErr := mysql.ParseGTIDSet(mySource.Flavor(), lastText)
		require.NoError(s.t, parseErr)
		require.True(s.t, storedSet.Contain(gtidAfter),
			"checkpoint GTID set %s does not cover the committed payload transaction %s", storedSet, gtidAfter)
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_MySQL_Binary_Trailing_Zeros reproduces trailing-0x00 truncation of fixed-length
// BINARY(N) columns over CDC.
//
// MySQL right-pads BINARY(N) to N bytes with 0x00. Internally this is represented
// as a binary array of length M=N-number_of_trailing(0x00). When reading from binlog these
// values need to be adapted back to N bytes based on the column type.
//
// See https://github.com/shyiko/mysql-binlog-connector-java/issues/169#issuecomment-301864569
// for a similar problem in a different project.
func (s ClickHouseSuite) Test_MySQL_Binary_Trailing_Zeros() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_binary_padding"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_binary_padding_dst"

	type binaryCase struct {
		id             int
		label          string
		beforeSnapshot bool
		// BINARY(16) covers the common short metadata path with a 1-byte row length.
		b16InsertHex string
		b16WantHex   string
		// BINARY(255) covers the upper edge of the same metadata path without switching
		// to MySQL's 2-byte row length encoding for fixed strings longer than 255 bytes.
		// MySQL does not allow BINARY(N) with N > 255.
		b255InsertHex string
		b255WantHex   string
	}

	cases := []binaryCase{
		// SELECT path: correct today, included to prove snapshot and CDC agree.
		{
			id:             1,
			label:          "snapshot_trailing_zero",
			beforeSnapshot: true,
			b16InsertHex:   strings.Repeat("11", 15) + "00",
			b16WantHex:     strings.Repeat("11", 15) + "00",
			b255InsertHex:  strings.Repeat("11", 254) + "00",
			b255WantHex:    strings.Repeat("11", 254) + "00",
		},
		// CDC path: MySQL packs these shorter than their declared BINARY(N) length.
		{
			id:             2,
			label:          "cdc_trailing_zero",
			beforeSnapshot: false,
			b16InsertHex:   "21" + strings.Repeat("11", 14) + "00",
			b16WantHex:     "21" + strings.Repeat("11", 14) + "00",
			b255InsertHex:  "21" + strings.Repeat("11", 253) + "00",
			b255WantHex:    "21" + strings.Repeat("11", 253) + "00",
		},
		{
			id:             3,
			label:          "cdc_multiple_trailing_zeros",
			beforeSnapshot: false,
			b16InsertHex:   "22" + strings.Repeat("11", 11) + strings.Repeat("00", 4),
			b16WantHex:     "22" + strings.Repeat("11", 11) + strings.Repeat("00", 4),
			b255InsertHex:  "22" + strings.Repeat("11", 250) + strings.Repeat("00", 4),
			b255WantHex:    "22" + strings.Repeat("11", 250) + strings.Repeat("00", 4),
		},
		{
			id:             4,
			label:          "cdc_all_zero",
			beforeSnapshot: false,
			b16InsertHex:   strings.Repeat("00", 16),
			b16WantHex:     strings.Repeat("00", 16),
			b255InsertHex:  strings.Repeat("00", 255),
			b255WantHex:    strings.Repeat("00", 255),
		},
		{
			id:             5,
			label:          "cdc_short_insert",
			beforeSnapshot: false,
			b16InsertHex:   "41",
			b16WantHex:     "41" + strings.Repeat("00", 15),
			b255InsertHex:  "42",
			b255WantHex:    "42" + strings.Repeat("00", 254),
		},
		// Controls: interior zero bytes and non-zero tails should be preserved without extra padding.
		{
			id:             6,
			label:          "cdc_interior_zero_nonzero_tail",
			beforeSnapshot: false,
			b16InsertHex:   "FF00112233445566778899AABBCCDDEE",
			b16WantHex:     "FF00112233445566778899AABBCCDDEE",
			b255InsertHex:  "FF00" + strings.Repeat("7F", 252) + "EE",
			b255WantHex:    "FF00" + strings.Repeat("7F", 252) + "EE",
		},
	}

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			label TEXT NOT NULL,
			b16 BINARY(16) NOT NULL,
			b255 BINARY(255) NOT NULL
		)
	`, quotedSrcFullName)))

	var snapshotCases []binaryCase
	var cdcCases []binaryCase
	for _, tc := range cases {
		if tc.beforeSnapshot {
			snapshotCases = append(snapshotCases, tc)
		} else {
			cdcCases = append(cdcCases, tc)
		}
	}
	snapshotValues := make([]string, 0, len(snapshotCases))
	for _, tc := range snapshotCases {
		snapshotValues = append(snapshotValues, fmt.Sprintf(
			"(%d,'%s',UNHEX('%s'),UNHEX('%s'))",
			tc.id, tc.label, tc.b16InsertHex, tc.b255InsertHex))
	}
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id,label,b16,b255) VALUES %s`, quotedSrcFullName, strings.Join(snapshotValues, ","))))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for snapshot row", dstTableName, "id,label,b16,b255", len(snapshotCases))

	cdcValues := make([]string, 0, len(cdcCases))
	for _, tc := range cdcCases {
		cdcValues = append(cdcValues, fmt.Sprintf(
			"(%d,'%s',UNHEX('%s'),UNHEX('%s'))",
			tc.id, tc.label, tc.b16InsertHex, tc.b255InsertHex))
	}
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id,label,b16,b255) VALUES %s`, quotedSrcFullName, strings.Join(cdcValues, ","))))

	EnvWaitForCount(env, s, "waiting for cdc rows", dstTableName, "id,label,b16,b255", len(cases))

	rows, err := s.GetRows(dstTableName, "id,label,b16,b255")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, len(cases), "expected all binary padding test rows")

	byLabel := make(map[string]map[string][]byte, len(rows.Records))
	for _, row := range rows.Records {
		label, ok := row[1].Value().(string)
		require.True(s.t, ok, "label should be a string")
		byLabel[label] = make(map[string][]byte, 2)
		for _, col := range []struct {
			name string
			idx  int
		}{
			{name: "b16", idx: 2},
			{name: "b255", idx: 3},
		} {
			switch v := row[col.idx].Value().(type) {
			case string:
				byLabel[label][col.name] = []byte(v)
			case []byte:
				byLabel[label][col.name] = v
			default:
				require.Failf(s.t, "unexpected type for binary column",
					"label=%s column=%s type=%T", label, col.name, row[col.idx].Value())
			}
		}
	}

	for _, tc := range cases {
		for _, col := range []struct {
			name    string
			wantHex string
		}{
			{name: "b16", wantHex: tc.b16WantHex},
			{name: "b255", wantHex: tc.b255WantHex},
		} {
			want, decErr := hex.DecodeString(col.wantHex)
			require.NoError(s.t, decErr)
			gotByCol, ok := byLabel[tc.label]
			require.Truef(s.t, ok, "missing row %q in destination", tc.label)
			got, ok := gotByCol[col.name]
			require.Truef(s.t, ok, "missing column %q for row %q in destination", col.name, tc.label)
			require.Equalf(s.t, want, got,
				"%s %q: MySQL stores %d bytes (%s) but ClickHouse has %d bytes (%s)",
				col.name, tc.label, len(want), col.wantHex, len(got), strings.ToUpper(hex.EncodeToString(got)))
		}
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Enum() {
	if mySource, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	} else {
		cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("only applies to mysql versions with binlog_row_metadata")
		}
	}

	srcTableName := "test_my_enum"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_my_enum_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			"key" TEXT NOT NULL,
			e enum('a','b''s', 'c') NOT NULL,
			s set('a','b','c') NOT NULL
		)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",e,s) VALUES
		('init','b''s','a,b'),('init','','')`, quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,\"key\",e,s")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`INSERT INTO %s ("key",e,s) VALUES
		('cdc','b''s','a,b'),('cdc','','')`, quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,\"key\",e,s")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Enum_Consistency() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_my_enum_consistency"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_my_enum_consistency_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			status ENUM('active', 'inactive', 'pending') NOT NULL
		)
	`, srcFullName)))

	// Insert row before snapshot
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (status) VALUES ('active')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for snapshot row to appear in destination
	EnvWaitForCount(env, s, "waiting on snapshot", dstTableName, "id,status", 1)

	// Insert row via CDC — on old MySQL this comes as integer from binlog
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (status) VALUES ('active')`, srcFullName)))

	// Wait for CDC row
	EnvWaitForCount(env, s, "waiting on cdc", dstTableName, "id,status", 2)

	// Verify both rows have the same status value (consistency between snapshot and CDC)
	rows, err := s.GetRows(dstTableName, "id,status")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2)
	require.Equal(s.t, rows.Records[0][1].Value(), rows.Records[1][1].Value(),
		"snapshot and CDC enum values should be consistent")
	if mysqlEnumUsesOrdinals(mySource) {
		require.EqualValues(s.t, 1, rows.Records[0][1].Value())
	} else {
		require.Equal(s.t, "active", rows.Records[0][1].Value())
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Enum_Consistency_Version0() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_my_enum_consistency_v0"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_my_enum_consistency_v0_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			status ENUM('active', 'inactive', 'pending') NOT NULL
		)
	`, srcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (status) VALUES ('active')`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.Env = map[string]string{"PEERDB_FORCE_INTERNAL_VERSION": strconv.FormatUint(uint64(shared.InternalVersion_First), 10)}
	flowConnConfig.Version = shared.InternalVersion_First

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting on snapshot", dstTableName, "id,status", 1)

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (status) VALUES ('active')`, srcFullName)))

	EnvWaitForCount(env, s, "waiting on cdc", dstTableName, "id,status", 2)

	rows, err := s.GetRows(dstTableName, "id,status")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2)
	require.EqualValues(s.t, 1, rows.Records[0][0].Value())
	require.EqualValues(s.t, 2, rows.Records[1][0].Value())
	require.Equal(s.t, "active", rows.Records[0][1].Value())
	if mysqlEnumUsesOrdinals(mySource) {
		require.Equal(s.t, "1", rows.Records[1][1].Value())
	} else {
		require.Equal(s.t, "active", rows.Records[1][1].Value())
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Charset_Consistency() {
	if mySource, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	} else {
		// Transcoding relies on collation metadata in TABLE_MAP, only present with binlog_row_metadata.
		cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("only applies to mysql versions with binlog_row_metadata")
		}
	}

	srcTableName := "test_my_charset"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_my_charset_dst"

	// latin1 (Windows-1252) and gbk columns: their stored bytes are NOT UTF-8.
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			latin1_col VARCHAR(50) CHARACTER SET latin1 NOT NULL,
			gbk_col VARCHAR(50) CHARACTER SET gbk NOT NULL,
			latin1_enum ENUM('café', 'plain') CHARACTER SET latin1 NOT NULL,
			gbk_set SET('你好', '再见') CHARACTER SET gbk NOT NULL
		)
	`, quotedSrcFullName)))

	// Insert before snapshot. The source connection runs SET NAMES utf8mb4, so the server
	// transcodes these UTF-8 literals down into the columns' latin1/gbk storage encodings.
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (latin1_col, gbk_col, latin1_enum, gbk_set) VALUES ('café', '你好', 'café', '你好,再见')`,
		quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting on snapshot", dstTableName, "id,latin1_col,gbk_col,latin1_enum,gbk_set", 1)

	// Same values via CDC (binlog path).
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (latin1_col, gbk_col, latin1_enum, gbk_set) VALUES ('café', '你好', 'café', '你好,再见')`,
		quotedSrcFullName)))

	EnvWaitForCount(env, s, "waiting on cdc", dstTableName, "id,latin1_col,gbk_col,latin1_enum,gbk_set", 2)

	rows, err := s.GetRows(dstTableName, "id,latin1_col,gbk_col,latin1_enum,gbk_set")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2)

	// Snapshot (row 0) and CDC (row 1) must be byte-identical and correctly decoded.
	require.Equal(s.t, rows.Records[0][1].Value(), rows.Records[1][1].Value(),
		"snapshot and CDC latin1 values should be consistent")
	require.Equal(s.t, rows.Records[0][2].Value(), rows.Records[1][2].Value(),
		"snapshot and CDC gbk values should be consistent")
	require.Equal(s.t, rows.Records[0][3].Value(), rows.Records[1][3].Value(),
		"snapshot and CDC latin1 enum values should be consistent")
	require.Equal(s.t, rows.Records[0][4].Value(), rows.Records[1][4].Value(),
		"snapshot and CDC gbk set values should be consistent")
	require.Equal(s.t, "café", rows.Records[1][1].Value())
	require.Equal(s.t, "你好", rows.Records[1][2].Value())
	require.Equal(s.t, "café", rows.Records[1][3].Value())
	require.Equal(s.t, "你好,再见", rows.Records[1][4].Value())

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Vector() {
	mysource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}

	var createTableSQL, initialInsertSQL, cdcInsertSQL string
	switch mysource.Config.Flavor {
	case protos.MySqlFlavor_MYSQL_MYSQL:
		cmp, err := mysource.CompareServerVersion(s.t.Context(), "9.0.0")
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("VECTOR type is only supported in MySQL 9.0+")
		}
		createTableSQL = `CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, val VECTOR)`
		initialInsertSQL = `INSERT INTO %s (val) VALUES (to_vector('[1.1,1.0]'))`
		cdcInsertSQL = `INSERT INTO %s (val) VALUES (to_vector('[2.718, 1.414]'))`
	case protos.MySqlFlavor_MYSQL_MARIA:
		cmp, err := mysource.CompareServerVersion(s.t.Context(), "11.7.0")
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("VECTOR type is only supported in MariaDB 11.7+")
		}
		createTableSQL = `CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, val VECTOR(2))`
		initialInsertSQL = `INSERT INTO %s (val) VALUES (Vec_FromText('[1.1,1.0]'))`
		cdcInsertSQL = `INSERT INTO %s (val) VALUES (Vec_FromText('[2.718, 1.414]'))`
	default:
		require.FailNow(s.t, fmt.Sprintf("unsupported MySQL flavor: %s", mysource.Config.Flavor))
	}

	srcTableName := "test_vector"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_vector_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(createTableSQL, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(initialInsertSQL, quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,val")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(cdcInsertSQL, quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,val")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_MariaDB_UUID_INET() {
	mysource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}
	if mysource.Config.Flavor != protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("UUID/INET4/INET6 are MariaDB-only types")
	}
	// INET4 is the newest of the three (MariaDB 10.10); gating on it guarantees all exist.
	cmp, err := mysource.CompareServerVersion(s.t.Context(), "10.10.0")
	require.NoError(s.t, err)
	if cmp < 0 {
		s.t.Skip("UUID/INET4/INET6 require MariaDB 10.10+")
	}

	srcTableName := "test_uuid_inet"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_uuid_inet_dst"

	require.NoError(s.t, s.Source().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id serial PRIMARY KEY,
			u uuid,
			ip4 inet4,
			ip6 inet6
		)`, srcFullName)))

	variants := []struct{ u, ip4, ip6 string }{
		{"6ccd780c-baba-1026-9564-5b8c656024db", "192.168.0.1", "2001:db8::1"},
		{"00112233-4455-6677-8899-aabbccddeeff", "10.0.0.255", "::1"},
		{"00000000-0000-0000-0000-000000000000", "0.0.0.0", "::"},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", "255.255.255.255", "2001:db8:85a3::8a2e:370:7334"},
		{"123e4567-e89b-12d3-a456-426614174000", "127.0.0.1", "fe80::1"},
	}

	insertVariants := func() {
		for _, v := range variants {
			require.NoError(s.t, s.Source().Exec(s.t.Context(), fmt.Sprintf(
				`INSERT INTO %s (u, ip4, ip6) VALUES ('%s', '%s', '%s')`, srcFullName, v.u, v.ip4, v.ip6)))
		}
	}

	insertVariants()

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForCount(env, s, "waiting for initial snapshot", dstTableName, "id", len(variants))

	insertVariants()

	EnvWaitForCount(env, s, "waiting for cdc", dstTableName, "id", 2*len(variants))

	// GetRows orders by id, so snapshot rows (ids 1..N) come first, then CDC rows (N+1..2N);
	// both batches hold the same values in the same order.
	rows, err := s.GetRows(dstTableName, "id, u, ip4, ip6")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 2*len(variants))

	for i, row := range rows.Records {
		want := variants[i%len(variants)]
		require.Len(s.t, row, 4)
		require.Equal(s.t, want.u, fmt.Sprint(row[1].Value()), "uuid mismatch at row %d", i+1)
		require.Equal(s.t, want.ip4, fmt.Sprint(row[2].Value()), "inet4 mismatch at row %d", i+1)
		require.Equal(s.t, want.ip6, fmt.Sprint(row[3].Value()), "inet6 mismatch at row %d", i+1)
	}

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

// Test_MySQL_MariaTypes exercises the binlog ALTER TABLE ADD COLUMN path
// (processAlterTableQuery -> QkindFromMysqlColumnType) together with the row-metadata decode
// path for a broad spectrum of MySQL/MariaDB column types.
func (s ClickHouseSuite) Test_MySQL_AlterTableAddColumnTypes() {
	mysource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}
	// ENUM/SET columns added during CDC need binlog row metadata to resolve their member names.
	cmp, err := mysource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
	require.NoError(s.t, err)
	if cmp < 0 {
		s.t.Skip("ALTER ADD COLUMN enum/set decoding requires binlog row metadata")
	}

	srcTableName := "test_add_col_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_add_col_types_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY)", srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	cols := []struct{ name, def, val string }{
		// Integer widths: the signed minimum and unsigned maximum pin width + signedness.
		{"c_tinyint", "TINYINT", "-128"},
		{"c_tinyint_u", "TINYINT UNSIGNED", "255"},
		{"c_bool", "TINYINT(1)", "1"},
		{"c_smallint", "SMALLINT", "-32768"},
		{"c_smallint_u", "SMALLINT UNSIGNED", "65535"},
		{"c_mediumint", "MEDIUMINT", "-8388608"},
		{"c_mediumint_u", "MEDIUMINT UNSIGNED", "16777215"},
		{"c_int", "INT", "-2147483648"},
		{"c_int_u", "INT UNSIGNED", "4294967295"},
		{"c_bigint", "BIGINT", "-9223372036854775808"},
		{"c_bigint_u", "BIGINT UNSIGNED", "18446744073709551615"},
		{"c_integer", "INTEGER", "7"}, // synonym; normalizes to int through the parser
		// Approximate + exact numeric.
		{"c_float", "FLOAT", "1.5"},
		{"c_double", "DOUBLE PRECISION", "2.5"}, // synonym; normalizes to double
		{"c_decimal", "DECIMAL(10,2)", "123.45"},
		{"c_decimal_big", "DECIMAL(60,3)", "780780780.780"},
		// Bit.
		{"c_bit", "BIT(20)", "b'11100011100011100011'"},
		// Date and time.
		{"c_date", "DATE", "'2020-01-02'"},
		{"c_datetime", "DATETIME(3)", "'2020-01-02 03:04:05.678'"},
		{"c_timestamp", "TIMESTAMP(6)", "'2020-01-02 03:04:05.678901'"},
		{"c_time", "TIME", "'13:14:15'"},
		{"c_year", "YEAR", "2021"},
		// Strings.
		{"c_char", "CHAR(10)", "'abc'"},
		{"c_varchar", "VARCHAR(20)", "'hello world'"},
		{"c_text", "TEXT", "'some text'"},
		{"c_enum", "ENUM('a','b','c')", "'b'"},
		{"c_set", "SET('x','y','z')", "'x,z'"},
		// Binary. BINARY(4) holds exactly 4 bytes, so there is no trailing-zero padding to
		// reconcile (that case has its own test, Test_MySQL_Binary_Trailing_Zeros).
		{"c_binary", "BINARY(4)", "'abcd'"},
		{"c_varbinary", "VARBINARY(10)", "'binblob'"},
		{"c_blob", "BLOB", "'someblob'"},
		// JSON.
		{"c_json", "JSON", `'{"a":2}'`},
	}

	// MariaDB-specific alias spellings the TiDB parser normalizes to a supported canonical type.
	// These mirror the MariaDB synonyms in TestAlterTableTypes; they are valid DDL on MariaDB but
	// not (all) on MySQL, so they only run on MariaDB flavor.
	if mysource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		cols = append(cols, []struct{ name, def, val string }{
			// Integer synonyms INT1..INT8 / MIDDLEINT map to the sized integer.
			{"c_int1", "INT1", "100"},               // -> tinyint
			{"c_int2", "INT2", "30000"},             // -> smallint
			{"c_int3", "INT3", "8000000"},           // -> mediumint
			{"c_int4", "INT4", "2000000000"},        // -> int
			{"c_int8", "INT8", "9000000000000000"},  // -> bigint
			{"c_middleint", "MIDDLEINT", "8000000"}, // -> mediumint
			// Exact-numeric synonyms normalize to decimal.
			{"c_dec", "DEC(10,2)", "123.45"},
			{"c_fixed", "FIXED(10,2)", "678.90"},
			// Approximate-numeric synonym normalizes to double.
			{"c_real", "REAL", "3.14159"},
			// Char synonyms normalize to char.
			{"c_nchar", "NCHAR(10)", "'nchar'"},
			{"c_national_char", "NATIONAL CHAR(10)", "'natchar'"},
			// Varchar synonyms normalize to varchar.
			{"c_nvarchar", "NVARCHAR(20)", "'nvarchar val'"},
			{"c_char_varying", "CHAR VARYING(20)", "'char varying'"},
			{"c_varcharacter", "VARCHARACTER(20)", "'varcharacter'"},
			{"c_national_varchar", "NATIONAL VARCHAR(20)", "'nat varchar'"},
			// LONG / LONG VARCHAR normalize to mediumtext.
			{"c_long", "LONG", "'long text'"},
			{"c_long_varchar", "LONG VARCHAR", "'long varchar text'"},
		}...)
	}

	adds := make([]string, len(cols))
	names := make([]string, 0, len(cols)+1)
	vals := make([]string, len(cols))
	names = append(names, "id")
	for i, c := range cols {
		adds[i] = "ADD COLUMN " + c.name + " " + c.def
		names = append(names, c.name)
		vals[i] = c.val
	}

	// Single multi-column ALTER: processAlterTableQuery iterates every ADD COLUMN spec, so this
	// exercises the DDL parse for all types at once.
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("ALTER TABLE %s %s", srcFullName, strings.Join(adds, ", "))))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s (%s) VALUES (1, %s)", srcFullName, strings.Join(names, ", "), strings.Join(vals, ", "))))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc add column", srcTableName, dstTableName, strings.Join(names, ","))

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Numbers() {
	if mysource, ok := s.source.(*MySqlSource); !ok || mysource.Config.Flavor != protos.MySqlFlavor_MYSQL_MYSQL {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_float"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_float_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, num numeric, num603 numeric(60, 3), f32 float, f64 double precision, r real)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(num,num603,f32,f64,r)VALUES(1.23,780780780.780,1.41421,2.718281828459045,6.28319)`,
			quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,num,num603,f32,f64,r")

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO%s(num,num603,f32,f64,r)VALUES(1.23,780780780.780,1.41421,2.718281828459045,6.28319)`,
			quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id,num,num603,f32,f64,r")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Geometric_Types() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_mysql_geometric_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_mysql_geometric_types"

	// Create a table with a geometry column that can store any geometric type
	err := s.Source().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(
			id serial PRIMARY KEY,
			geometry_col GEOMETRY
		)`, srcFullName))
	require.NoError(s.t, err)

	// Insert test data with various geometric types
	err = s.Source().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (geometry_col) VALUES
			(ST_GeomFromText('POINT(1 2)')),
			(ST_GeomFromText('LINESTRING(1 2, 3 4)')),
			(ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))')),
			(ST_GeomFromText('MULTIPOINT(1 2, 3 4)')),
			(ST_GeomFromText('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))')),
			(ST_GeomFromText('MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))')),
			(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))')),
			(ST_GeomFromText('POINT(5 6)', 3857))`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_mysql_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 8)

	// Insert additional rows to test CDC
	err = s.Source().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (geometry_col) VALUES
		(ST_GeomFromText('POINT(10 20)')),
		(ST_GeomFromText('LINESTRING(10 20, 30 40)')),
		(ST_GeomFromText('POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))')),
		(ST_GeomFromText('POINT(30 40)', 3857))`, srcFullName))
	require.NoError(s.t, err)

	// Wait for CDC to replicate the new rows
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 12)

	// Verify that the data was correctly replicated
	rows, err := s.GetRows(dstTableName, "id, geometry_col")
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 12, "expected 12 rows")

	// Expected WKT format values for each geometric type.
	// SRID is intentionally dropped because ClickHouse doesn't support EWKT (SRID=N;WKT).
	expectedValues := []string{
		"POINT (1 2)",
		"LINESTRING (1 2, 3 4)",
		"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
		"MULTIPOINT ((1 2), (3 4))",
		"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))",
		"MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
		"GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))",
		"POINT (5 6)",
		"POINT (10 20)",
		"LINESTRING (10 20, 30 40)",
		"POLYGON ((10 10, 30 10, 30 30, 10 30, 10 10))",
		"POINT (30 40)",
	}

	for i, row := range rows.Records {
		require.Len(s.t, row, 2, "expected 2 columns")
		geometryVal := row[1].Value()
		require.Equal(s.t, expectedValues[i], geometryVal, "geometry_col value mismatch at row %d", i+1)
	}

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Specific_Geometric_Types() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_mysql_s_geometric_types"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	dstTableName := "test_mysql_s_geometric_types"

	// Create a table with a geometry column that can store any geometric type
	err := s.Source().Exec(s.t.Context(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s(
		id serial PRIMARY KEY,
		point_col POINT,
		linestring_col LINESTRING,
		polygon_col POLYGON,
		multipoint_col MULTIPOINT,
		multilinestring_col MULTILINESTRING,
		multipolygon_col MULTIPOLYGON,
		geometrycollection_col GEOMETRYCOLLECTION
	)`, srcFullName))
	require.NoError(s.t, err)

	// Insert test data with various geometric types
	err = s.Source().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (
			point_col,
			linestring_col,
			polygon_col,
			multipoint_col,
			multilinestring_col,
			multipolygon_col,
			geometrycollection_col
		) VALUES (
			ST_GeomFromText('POINT(1 2)'),
			ST_GeomFromText('LINESTRING(1 2, 3 4)'),
			ST_GeomFromText('POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))'),
			ST_GeomFromText('MULTIPOINT(1 2, 3 4)'),
			ST_GeomFromText('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))'),
			ST_GeomFromText('MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))'),
			ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))')
		), (
			ST_GeomFromText('POINT(5 6)', 3857),
			ST_GeomFromText('LINESTRING(5 6, 7 8)', 3857),
			ST_GeomFromText('POLYGON((5 5, 7 5, 7 7, 5 7, 5 5))', 3857),
			ST_GeomFromText('MULTIPOINT(5 6, 7 8)', 3857),
			ST_GeomFromText('MULTILINESTRING((5 6, 7 8), (9 10, 11 12))', 3857),
			ST_GeomFromText('MULTIPOLYGON(((5 5, 7 5, 7 7, 5 7, 5 5)), ((8 8, 10 8, 10 10, 8 10, 8 8)))', 3857),
			ST_GeomFromText('GEOMETRYCOLLECTION(POINT(5 6), LINESTRING(5 6, 7 8))', 3857)
		)`, srcFullName))
	require.NoError(s.t, err)

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix("clickhouse_test_mysql_geometric_types"),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}

	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Wait for initial snapshot to complete
	EnvWaitForCount(env, s, "waiting for initial snapshot count", dstTableName, "id", 2)

	// Insert additional rows to test CDC
	require.NoError(s.t, s.Source().Exec(s.t.Context(), fmt.Sprintf(`
	INSERT INTO %s (
		point_col,
		linestring_col,
		polygon_col,
		multipoint_col,
		multilinestring_col,
		multipolygon_col,
		geometrycollection_col
	) VALUES (
		ST_PointFromText('POINT(10 20)'),
		ST_LineFromText('LINESTRING(10 20, 30 40)'),
		ST_PolygonFromText('POLYGON((10 10, 30 10, 30 30, 10 30, 10 10))'),
		ST_MPointFromText('MULTIPOINT(10 20, 30 40)'),
		ST_MLineFromText('MULTILINESTRING((10 20, 30 40), (50 60, 70 80))'),
		ST_MPolyFromText('MULTIPOLYGON(((10 10, 30 10, 30 30, 10 30, 10 10)), ((40 40, 60 40, 60 60, 40 60, 40 40)))'),
		ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(10 20), LINESTRING(10 20, 30 40))')
	), (
		ST_GeomFromText('POINT(40 50)', 3857),
		ST_GeomFromText('LINESTRING(40 50, 60 70)', 3857),
		ST_GeomFromText('POLYGON((10 20, 30 20, 30 40, 10 40, 10 20))', 3857),
		ST_GeomFromText('MULTIPOINT(40 50, 60 70)', 3857),
		ST_GeomFromText('MULTILINESTRING((40 50, 60 70), (10 20, 30 40))', 3857),
		ST_GeomFromText('MULTIPOLYGON(((10 20, 30 20, 30 40, 10 40, 10 20)), ((50 60, 70 60, 70 80, 50 80, 50 60)))', 3857),
		ST_GeomFromText('GEOMETRYCOLLECTION(POINT(40 50), LINESTRING(40 50, 60 70))', 3857)
	)`, srcFullName)))

	// Wait for CDC to replicate the new rows
	EnvWaitForCount(env, s, "waiting for CDC count", dstTableName, "id", 4)

	// Verify that the data was correctly replicated
	rows, err := s.GetRows(dstTableName, `id, point_col, linestring_col, polygon_col, multipoint_col,
		multilinestring_col, multipolygon_col, geometrycollection_col`)
	require.NoError(s.t, err)
	require.Len(s.t, rows.Records, 4, "expected 4 rows")

	// Expected WKT format values for each geometric type.
	// SRID is intentionally dropped because ClickHouse doesn't support EWKT (SRID=N;WKT).
	expectedValues := [][]string{
		{
			"POINT (1 2)",
			"LINESTRING (1 2, 3 4)",
			"POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
			"MULTIPOINT ((1 2), (3 4))",
			"MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))",
			"MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
			"GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (1 2, 3 4))",
		},
		{
			"POINT (5 6)",
			"LINESTRING (5 6, 7 8)",
			"POLYGON ((5 5, 7 5, 7 7, 5 7, 5 5))",
			"MULTIPOINT ((5 6), (7 8))",
			"MULTILINESTRING ((5 6, 7 8), (9 10, 11 12))",
			"MULTIPOLYGON (((5 5, 7 5, 7 7, 5 7, 5 5)), ((8 8, 10 8, 10 10, 8 10, 8 8)))",
			"GEOMETRYCOLLECTION (POINT (5 6), LINESTRING (5 6, 7 8))",
		},
		{
			"POINT (10 20)",
			"LINESTRING (10 20, 30 40)",
			"POLYGON ((10 10, 30 10, 30 30, 10 30, 10 10))",
			"MULTIPOINT ((10 20), (30 40))",
			"MULTILINESTRING ((10 20, 30 40), (50 60, 70 80))",
			"MULTIPOLYGON (((10 10, 30 10, 30 30, 10 30, 10 10)), ((40 40, 60 40, 60 60, 40 60, 40 40)))",
			"GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40))",
		},
		{
			"POINT (40 50)",
			"LINESTRING (40 50, 60 70)",
			"POLYGON ((10 20, 30 20, 30 40, 10 40, 10 20))",
			"MULTIPOINT ((40 50), (60 70))",
			"MULTILINESTRING ((40 50, 60 70), (10 20, 30 40))",
			"MULTIPOLYGON (((10 20, 30 20, 30 40, 10 40, 10 20)), ((50 60, 70 60, 70 80, 50 80, 50 60)))",
			"GEOMETRYCOLLECTION (POINT (40 50), LINESTRING (40 50, 60 70))",
		},
	}

	for i, row := range rows.Records {
		require.Len(s.t, row, 8, "expected 8 columns")
		for j := 1; j < 8; j++ {
			geometryVal := row[j].Value()
			require.Equal(s.t, expectedValues[i][j-1], geometryVal, "geometry value mismatch at row %d column %d", i+1, j)
		}
	}

	// Clean up
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Schema_Changes() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	t := s.T()
	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	srcTable := "test_mysql_schema_changes"
	dstTable := "test_mysql_schema_changes_dst"
	srcTableName := AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)
	secondSrcTable := "test_mysql_schema_changes_second"
	secondDstTable := "test_mysql_schema_changes_second_dst"
	secondSrcTableName := AttachSchema(s, secondSrcTable)

	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			c1 BIGINT
		);
	`, srcTableName)))
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY
		);
	`, secondSrcTableName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:   AddSuffix(s, srcTable),
		TableMappings: TableMappings(s, srcTable, dstTable, secondSrcTable, secondDstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	// wait for PeerFlowStatusQuery to finish setup
	// and then insert and mutate schema repeatedly.
	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES(1)`, srcTableName)))

	EnvWaitForEqualTablesWithNames(env, s, "normalize reinsert", srcTable, dstTable, "id,c1")

	expectedTableSchema := ExpectedDestinationSchema(s, dstTable, []*protos.FieldDescription{
		{Name: ExpectedDestinationIdentifier(s, "id"), Type: string(types.QValueKindUInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "c1"), Type: string(types.QValueKindInt64), TypeModifier: -1},
	})
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	EnvNoError(t, env, err)
	EnvTrue(t, env, RequireEqualTableSchemas(t, expectedTableSchema, output[dstTableName]))

	// alter source table, add column addedColumn and insert another row.
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf("ALTER TABLE %s ADD COLUMN `addedColumn` BIGINT", srcTableName)))
	// so that the batch finishes, insert a row into the second source table.
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s VALUES(DEFAULT)`, secondSrcTableName)))
	EnvWaitForEqualTablesWithNames(env, s, "normalize altered row", srcTable, dstTable, "id,c1,coalesce(`addedColumn`,0) `addedColumn`")
	expectedTableSchema = ExpectedDestinationSchema(s, dstTable, []*protos.FieldDescription{
		{Name: ExpectedDestinationIdentifier(s, "id"), Type: string(types.QValueKindUInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "c1"), Type: string(types.QValueKindInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "addedColumn"), Type: string(types.QValueKindInt64), TypeModifier: -1},
	})
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	EnvNoError(t, env, err)
	EnvTrue(t, env, RequireEqualTableSchemas(t, expectedTableSchema, output[dstTableName]))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s ClickHouseSuite) Test_MySQL_GhOst_Schema_Changes() {
	if mySource, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	} else {
		cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
		require.NoError(s.t, err)
		if cmp < 0 {
			s.t.Skip("only applies to mysql versions with binlog_row_metadata support")
		}
	}

	t := s.T()
	destinationSchemaConnector, ok := s.DestinationConnector().(connectors.GetTableSchemaConnector)
	if !ok {
		t.Skip("skipping test because destination connector does not implement GetTableSchemaConnector")
	}

	fixedBinaryInsertHex := "FF000102030405060708090A0B0C"
	fixedBinaryWantHex := fixedBinaryInsertHex + "0000"
	varBinaryWantHex := "FE000102030405060708090A0B0C0D00"

	srcTable := "test_mysql_ghost_schema"
	dstTable := "test_mysql_ghost_schema_dst"
	srcTableName := AttachSchema(s, srcTable)
	dstTableName := s.DestinationTable(dstTable)

	// Ghost table names (like gh-ost creates)
	ghostTable := "_" + srcTable + "_gho"
	ghostTableName := AttachSchema(s, ghostTable)
	oldTable := "_" + srcTable + "_del"
	oldTableName := AttachSchema(s, oldTable)

	// Create initial table with id and c1
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			c1 BIGINT
		)
	`, srcTableName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      AddSuffix(s, srcTable),
		TableNameMapping: map[string]string{AttachSchema(s, srcTable): dstTable},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := NewTemporalClient(t)
	env := ExecutePeerflow(t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(t, env, flowConnConfig)

	// Insert initial row
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES(1)`, srcTableName)))
	EnvWaitForEqualTablesWithNames(env, s, "initial row", srcTable, dstTable, "id,c1")

	// Verify initial schema
	expectedTableSchema := ExpectedDestinationSchema(s, dstTable, []*protos.FieldDescription{
		{Name: ExpectedDestinationIdentifier(s, "id"), Type: string(types.QValueKindUInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "c1"), Type: string(types.QValueKindInt64), TypeModifier: -1},
	})
	output, err := destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	EnvNoError(t, env, err)
	EnvTrue(t, env, RequireEqualTableSchemas(t, expectedTableSchema, output[dstTableName]))

	// ============================================
	// Simulate gh-ost migration: add columns to test type detection
	// - c2: BIGINT (basic signed integer)
	// - c3: INT UNSIGNED (tests unsigned flag)
	// - c4: BLOB (tests binary charset -> QValueKindBytes)
	// - c5: TEXT (tests non-binary charset -> QValueKindString)
	// - c6: DECIMAL (no precision/scale -> typmod -1)
	// - c7: DECIMAL(10,2) (tests precision/scale extraction)
	// - c8: DECIMAL(18,6) (tests different precision/scale)
	// - c9: BINARY(16) (tests binary charset on STRING metadata type)
	// - c10: VARBINARY(16) (tests binary charset on VAR_STRING metadata type)
	// ============================================

	// 1. gh-ost creates ghost table with new schema (original + new columns)
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			c1 BIGINT,
			c2 BIGINT,
			c3 INT UNSIGNED,
			c4 BLOB,
			c5 TEXT,
			c6 DECIMAL,
			c7 DECIMAL(10,2),
			c8 DECIMAL(18,6),
			c9 BINARY(16),
			c10 VARBINARY(16)
		)
	`, ghostTableName)))

	// 2. gh-ost copies existing data to ghost table (we simulate this)
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
		SELECT id, c1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM %s
	`, ghostTableName, srcTableName)))

	// 3. Insert another row into original table (gh-ost would capture this via binlog and apply to ghost)
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`INSERT INTO %s(c1) VALUES(2)`, srcTableName)))
	// Simulate gh-ost applying it to ghost table
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) VALUES(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
		ghostTableName)))
	EnvWaitForEqualTablesWithNames(env, s, "pre-cutover row", srcTable, dstTable, "id,c1")

	// 4. gh-ost atomic cut-over: rename both tables simultaneously
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(`
		RENAME TABLE %s TO %s, %s TO %s
	`, srcTableName, oldTableName, ghostTableName, srcTableName)))

	// 5. Insert a row with the new columns populated (this goes to the new table, formerly ghost)
	EnvNoError(t, env, s.Source().Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %s(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
		VALUES(3, 300, 400, x'deadbeef', 'hello text', 123.45, 12345.67, 123456.789012, UNHEX('%s'), UNHEX('%s'))`,
		srcTableName, fixedBinaryInsertHex, varBinaryWantHex)))
	EnvWaitForEqualTablesWithNames(env, s, "post-cutover row", srcTable, dstTable,
		"id,c1,coalesce(c2,0) c2,coalesce(c4,'') c4,coalesce(c5,'') c5,coalesce(c6,0) c6,coalesce(c7,0) c7,coalesce(c8,0) c8")

	// Verify c3 separately - MariaDB returns a different type here than MySQL and breaks coalesce
	dstRows, err := s.GetRows(dstTable, "id,c3")
	require.NoError(t, err)
	require.Len(t, dstRows.Records, 3)
	require.Equal(t, uint32(400), dstRows.Records[2][1].Value())

	dstRows, err = s.GetRows(dstTable, "id,c9,c10")
	require.NoError(t, err)
	require.Len(t, dstRows.Records, 3)

	qvalueBytes := func(qv types.QValue) []byte {
		switch v := qv.Value().(type) {
		case string:
			return []byte(v)
		case []byte:
			return v
		default:
			require.Failf(t, "unexpected binary column type", "type=%T value=%v", qv.Value(), qv.Value())
			return nil
		}
	}

	fixedBinaryWant, err := hex.DecodeString(fixedBinaryWantHex)
	require.NoError(t, err)
	varBinaryWant, err := hex.DecodeString(varBinaryWantHex)
	require.NoError(t, err)
	require.Equal(t, fixedBinaryWant, qvalueBytes(dstRows.Records[2][1]))
	require.Equal(t, varBinaryWant, qvalueBytes(dstRows.Records[2][2]))

	// Verify schema was updated to include the new columns.
	// BLOB/BINARY/VARBINARY are stored in ClickHouse as String,
	// our CH GetTableSchemaForTable always returns TypeModifier -1 for decimals
	expectedTableSchema = ExpectedDestinationSchema(s, dstTable, []*protos.FieldDescription{
		{Name: ExpectedDestinationIdentifier(s, "id"), Type: string(types.QValueKindUInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "c1"), Type: string(types.QValueKindInt64), TypeModifier: -1},
		{Name: ExpectedDestinationIdentifier(s, "c2"), Type: string(types.QValueKindInt64), TypeModifier: -1},   // BIGINT
		{Name: ExpectedDestinationIdentifier(s, "c3"), Type: string(types.QValueKindUInt32), TypeModifier: -1},  // INT UNSIGNED
		{Name: ExpectedDestinationIdentifier(s, "c4"), Type: string(types.QValueKindString), TypeModifier: -1},  // BLOB (binary charset)
		{Name: ExpectedDestinationIdentifier(s, "c5"), Type: string(types.QValueKindString), TypeModifier: -1},  // TEXT (non-binary charset)
		{Name: ExpectedDestinationIdentifier(s, "c6"), Type: string(types.QValueKindNumeric), TypeModifier: -1}, // DECIMAL (default 10,0)
		{Name: ExpectedDestinationIdentifier(s, "c7"), Type: string(types.QValueKindNumeric), TypeModifier: -1}, // DECIMAL(10,2)
		{Name: ExpectedDestinationIdentifier(s, "c8"), Type: string(types.QValueKindNumeric), TypeModifier: -1}, // DECIMAL(18,6)
		{Name: ExpectedDestinationIdentifier(s, "c9"), Type: string(types.QValueKindString), TypeModifier: -1},  // BINARY(16)
		{Name: ExpectedDestinationIdentifier(s, "c10"), Type: string(types.QValueKindString), TypeModifier: -1}, // VARBINARY(16)
	})
	output, err = destinationSchemaConnector.GetTableSchema(t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: dstTableName}})
	EnvNoError(t, env, err)
	EnvTrue(t, env, RequireEqualTableSchemas(t, expectedTableSchema, output[dstTableName]))

	env.Cancel(t.Context())
	RequireEnvCanceled(t, env)
}

func (s ClickHouseSuite) Test_MySQL_NumToVarcharCoercion() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_coercion"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_coercion_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, num bigint, f32 float, f64 double precision)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO %s(num,f32,f64)VALUES(780780780,1.41421,2.718281828459045)`, quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,num,f32,f64")

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s MODIFY COLUMN num VARCHAR(20), MODIFY COLUMN f32 VARCHAR(20), MODIFY COLUMN f64 VARCHAR(20)
	`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf(`INSERT INTO%s(num,f32,f64)VALUES('780780780','1.41421','2.718281828459045')`, quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id")

	var numCount, f32Count, f64Count uint64
	ch, err := connclickhouse.Connect(s.t.Context(), nil, s.Peer().GetClickhouseConfig())
	require.NoError(s.t, err)
	require.NoError(s.t, ch.QueryRow(s.t.Context(),
		"SELECT count(distinct num), count(distinct f32), count(distinct f64) FROM "+clickhouse.QuoteIdentifier(dstTableName),
	).Scan(&numCount, &f32Count, &f64Count))
	require.Equal(s.t, uint64(1), numCount)
	require.Equal(s.t, uint64(1), f32Count)
	require.Equal(s.t, uint64(1), f64Count)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_DateCoercion() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTableName := "test_date_coercion"
	srcFullName := s.attachSchemaSuffix(srcTableName)
	quotedSrcFullName := "\"" + strings.ReplaceAll(srcFullName, ".", "\".\"") + "\""
	dstTableName := "test_date_coercion_dst"

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			d_pre1970 DATE NOT NULL,
			d_post1970 DATE NOT NULL,
			d_zero DATE NOT NULL,
			d_zero_month DATE NOT NULL,
			d_zero_day DATE NOT NULL,
			d_dt3 DATE NOT NULL,
			d_dt6 DATE NOT NULL,
			d_ts DATE NOT NULL,
			d_ts3 DATE NOT NULL,
			d_ts6 DATE NOT NULL
		)`, quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (d_pre1970, d_post1970, d_zero, d_zero_month, d_zero_day, d_dt3, d_dt6, d_ts, d_ts3, d_ts6) VALUES
			('1926-02-02', '2025-02-02', '0000-00-00', '2000-00-01', '2000-01-00',
			'1926-02-02', '1926-02-02', '2025-02-02', '2025-02-02', '2025-02-02')`,
		quotedSrcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      s.attachSuffix(srcTableName),
		TableNameMapping: map[string]string{srcFullName: dstTableName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial",
		srcTableName, dstTableName, "id,d_pre1970,d_post1970,d_zero,d_zero_month,d_zero_day,d_dt3,d_dt6,d_ts,d_ts3,d_ts6")

	// NOTE: internally in MySQL these are DATETIME2 and TIMESTAMP2,
	// the older DATETIME and TIMESTAMP are pre-2013
	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(`
		ALTER TABLE %s
			MODIFY COLUMN d_pre1970 DATETIME NOT NULL,
			MODIFY COLUMN d_post1970 DATETIME NOT NULL,
			MODIFY COLUMN d_zero DATETIME NOT NULL,
			MODIFY COLUMN d_zero_month DATETIME NOT NULL,
			MODIFY COLUMN d_zero_day DATETIME NOT NULL,
			MODIFY COLUMN d_dt3 DATETIME(3) NOT NULL,
			MODIFY COLUMN d_dt6 DATETIME(6) NOT NULL,
			MODIFY COLUMN d_ts TIMESTAMP NOT NULL,
			MODIFY COLUMN d_ts3 TIMESTAMP(3) NOT NULL,
			MODIFY COLUMN d_ts6 TIMESTAMP(6) NOT NULL`,
		quotedSrcFullName)))

	require.NoError(s.t, s.source.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (
			d_pre1970, d_post1970,
			d_zero, d_zero_month, d_zero_day,
			d_dt3, d_dt6,
			d_ts, d_ts3, d_ts6
		) VALUES (
		 	'1926-02-02 03:00:00', '2025-02-02 03:00:00',
			'0000-00-00 00:00:00', '2000-00-01 12:00:00', '2000-01-00 12:00:00',
			'1926-02-02 03:00:00.123', '1926-02-02 03:00:00.123456',
			'2025-02-02 03:00:00', '2025-02-02 03:00:00.654', '2025-02-02 03:00:00.654321'
		)`,
		quotedSrcFullName)))

	EnvWaitForEqualTablesWithNames(env, s, "waiting on cdc", srcTableName, dstTableName, "id")

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Column_Position_Shifting_DDL_Error() {
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}

	cmp, err := mySource.CompareServerVersion(s.t.Context(), mysql_validation.MySQLMinVersionForBinlogRowMetadata)
	require.NoError(s.t, err)
	if cmp >= 0 {
		s.t.Skip("only applies to mysql versions WITHOUT binlog_row_metadata support")
	}

	testCases := []struct {
		name   string
		ddlSQL string
	}{
		{"drop_column", "DROP COLUMN c1"},
		{"add_column_after", "ADD COLUMN new_col INT AFTER id"},
		{"modify_column_first", "MODIFY COLUMN c1 INT FIRST"},
		{"change_column_after", "CHANGE COLUMN c1 c1_new INT AFTER id"},
	}

	for _, tc := range testCases {
		s.t.Run(tc.name, func(t *testing.T) {
			srcTableName := "test_position_shift_" + tc.name
			srcFullName := s.attachSchemaSuffix(srcTableName)
			dstTableName := fmt.Sprintf("test_position_shift_%s_dst", tc.name)

			require.NoError(t, s.source.Exec(s.t.Context(), fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, c1 INT)`, srcFullName)))

			require.NoError(t, s.source.Exec(s.t.Context(), fmt.Sprintf(
				`INSERT INTO %s (c1) VALUES (1)`, srcFullName)))

			connectionGen := FlowConnectionGenerationConfig{
				FlowJobName:      s.attachSuffix(srcTableName),
				TableNameMapping: map[string]string{srcFullName: dstTableName},
				Destination:      s.Peer().Name,
			}
			flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
			flowConnConfig.DoInitialSnapshot = true

			temporalClient := NewTemporalClient(s.t)
			env := ExecutePeerflow(s.t, temporalClient, flowConnConfig)
			SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)
			EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTableName, dstTableName, "id,c1")

			// Execute position-shifting DDL - this should cause an error
			require.NoError(t, s.source.Exec(s.t.Context(), fmt.Sprintf(
				`ALTER TABLE %s %s`, srcFullName, tc.ddlSQL)))

			catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(s.t.Context())
			require.NoError(t, err)
			EnvWaitFor(s.t, env, 3*time.Minute, "waiting for error message", func() bool {
				rows, err := catalogPool.Query(s.t.Context(), `
					SELECT COUNT(*) FROM peerdb_stats.flow_errors
					WHERE error_type='error'
					AND flow_name = $1
					AND error_message ILIKE '%Detected position-shifting DDL on table %'
				`, flowConnConfig.FlowJobName)
				if err != nil {
					t.Log("Error querying flow_errors:", err)
					return false
				}
				defer rows.Close()

				if rows.Next() {
					var count int64
					if err := rows.Scan(&count); err != nil {
						t.Log("Error scanning count:", err)
						return false
					}
					return count > 0
				}
				return false
			})

			env.Cancel(s.t.Context())
			RequireEnvCanceled(s.t, env)
		})
	}
}

// Test_MySQL_BinlogIncident verifies that a MySQL binlog INCIDENT_EVENT (LOST_EVENTS) surfaces as
// a resync-required error on the mirror. It runs its own throwaway debug-build MySQL because the
// incident must be injected via the DBUG facility and requires a custom image.
func (s ClickHouseSuite) Test_MySQL_BinlogIncident() {
	if s.cluster {
		s.t.Skip("source-side incident coverage does not need to run against ClickHouse cluster")
	}
	mySource, ok := s.source.(*MySqlSource)
	if !ok {
		s.t.Skip("only applies to mysql")
	}
	if mySource.Config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		s.t.Skip("binlog incident injection requires a MySQL debug build; not available for MariaDB")
	}

	req := testcontainers.ContainerRequest{
		Image: "ghcr.io/peerdb-io/mysql-debug:8.0.46",
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": internal.MySQLTestRootPasswordWithFallback("cipass"),
			"MYSQL_ROOT_HOST":     "%",
		},
		// Keep the debug server small for the one-off testcontainers
		Cmd: []string{
			"mysqld",
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--innodb-buffer-pool-size=64M",
			"--performance-schema=OFF",
			"--mysqlx=0",
			"--max-connections=20",
			"--table-open-cache=64",
			"--table-definition-cache=128",
			"--innodb-log-buffer-size=8M",
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForListeningPort("3306/tcp").WithStartupTimeout(3 * time.Minute),
	}

	ctr, err := testcontainers.GenericContainer(s.t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	testcontainers.CleanupContainer(s.t, ctr, testcontainers.StopTimeout(30*time.Second))
	require.NoError(s.t, err)

	mapped, err := ctr.MappedPort(s.t.Context(), "3306/tcp")
	require.NoError(s.t, err)
	port, err := strconv.Atoi(mapped.Port())
	require.NoError(s.t, err)

	suffix := "mydbginc_" + strings.ToLower(common.RandomString(8))
	config := &protos.MySqlConfig{
		// host.docker.internal resolves both from the test process (to the published port) and
		// from the flow worker container (via host-gateway), unlike the container's own host.
		Host:                 internal.MySQLTestHost(),
		Port:                 uint32(port),
		User:                 "root",
		Password:             internal.MySQLTestRootPasswordWithFallback("cipass"),
		DisableTls:           true,
		Flavor:               protos.MySqlFlavor_MYSQL_MYSQL,
		ReplicationMechanism: mySource.Config.ReplicationMechanism,
	}
	src, err := setupMyConnector(s.t, suffix, config, "mysql_debug_"+suffix)
	require.NoError(s.t, err)
	s.t.Cleanup(func() { src.Teardown(s.t, context.Background(), suffix) })

	srcTableName := "incident"
	srcFullName := fmt.Sprintf("e2e_test_%s.%s", suffix, srcTableName)
	dstTableName := "incident_dst"

	require.NoError(s.t, src.Exec(s.t.Context(), fmt.Sprintf(
		`CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)`, srcFullName)))

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: "test_mysql_binlog_incident_" + suffix,
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      srcFullName,
			DestinationTableIdentifier: dstTableName,
			ShardingKey:                "id",
		}},
		Destination: s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	// The suite source is the shared CI MySQL; point the mirror at our debug source instead.
	flowConnConfig.SourceName = src.GeneratePeer(s.t).Name

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// Inject a binlog incident (LOST_EVENTS). The DBUG symbol takes effect for the session that
	// runs the committing statement, so the SET and the INSERT must share a connection - the
	// connector reuses a single connection, so sequential Exec calls satisfy this.
	require.NoError(s.t, src.Exec(s.t.Context(), "SET SESSION debug='+d,binlog_inject_incident'"))
	require.NoError(s.t, src.Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'incident')`, srcFullName)))
	require.NoError(s.t, src.Exec(s.t.Context(), "SET SESSION debug='-d,binlog_inject_incident'"))

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(s.t.Context())
	require.NoError(s.t, err)
	EnvWaitFor(s.t, env, 3*time.Minute, "waiting for binlog incident error", func() bool {
		count, err := GetLogCount(s.t.Context(), catalogPool, flowConnConfig.FlowJobName, "error", "binlog incident event received")
		if err != nil {
			s.t.Log("Error querying flow_errors:", err)
			return false
		}
		return count > 0
	})

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_Default_Partition_Key_Parallel_Snapshot() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTable := "test_default_partition_key_parallel"
	srcFullName := s.attachSchemaSuffix(srcTable)
	dstTable := "test_default_partition_key_parallel_dst"

	const numRows = 100
	const numPartitions = 8
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)", srcFullName)))
	for i := 1; i <= numRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", srcFullName, i)))
	}

	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("default_partition_key_parallel"),
		// PartitionKey is not specified to test default partition key detection
		TableMappings: TableMappings(s, srcTable, dstTable),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumPartitionsOverride = numPartitions

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTable, dstTable, "id,created_at")

	partitionRows, err := s.catalog.Query(s.t.Context(),
		`SELECT rows_in_partition FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	defer partitionRows.Close()

	var partitionCount int32
	var totalRows int64
	for partitionRows.Next() {
		var rowsInPartition int64
		require.NoError(s.t, partitionRows.Scan(&rowsInPartition))
		totalRows += rowsInPartition
		partitionCount++
	}
	require.NoError(s.t, partitionRows.Err())
	require.EqualValues(s.t, numPartitions, partitionCount)
	require.EqualValues(s.t, numRows, totalRows)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_String_Partition_Key_UUID_Parallel_Snapshot() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTable := "test_string_pk_uuid"
	srcFullName := s.attachSchemaSuffix(srcTable)
	dstTable := "test_string_pk_uuid_dst"

	const numRows = 100
	const numPartitions = 8
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s (id CHAR(36) PRIMARY KEY, val INT NOT NULL)", srcFullName)))
	for i := 1; i <= numRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s (id, val) VALUES ('%s', %d)", srcFullName, uuid.NewString(), i)))
	}

	tableMappings := TableMappings(s, srcTable, dstTable)
	// a String column can't be used directly as a Distributed sharding key (ClickHouse
	// requires an integer-typed sharding expression), so hash it for the cluster suite
	for _, tm := range tableMappings {
		tm.ShardingKey = "cityHash64(id)"
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("string_pk_uuid"),
		// PartitionKey is not specified to test default partition key detection
		TableMappings: tableMappings,
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumPartitionsOverride = numPartitions

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTable, dstTable, "id,val")

	partitionRows, err := s.catalog.Query(s.t.Context(),
		`SELECT rows_in_partition FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	defer partitionRows.Close()

	var partitionCount int32
	var totalRows int64
	for partitionRows.Next() {
		var rowsInPartition int64
		require.NoError(s.t, partitionRows.Scan(&rowsInPartition))
		totalRows += rowsInPartition
		partitionCount++
	}
	require.NoError(s.t, partitionRows.Err())
	require.EqualValues(s.t, numPartitions, partitionCount)
	require.EqualValues(s.t, numRows, totalRows)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s ClickHouseSuite) Test_MySQL_String_Partition_Key_Arbitrary_FullTable() {
	if _, ok := s.source.(*MySqlSource); !ok {
		s.t.Skip("only applies to mysql")
	}

	srcTable := "test_string_pk_non_uuid"
	srcFullName := s.attachSchemaSuffix(srcTable)
	dstTable := "test_string_pk_non_uuid_dst"

	const numRows = 100
	const numPartitions = 8
	require.NoError(s.t, s.source.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s (id VARCHAR(50) PRIMARY KEY, val INT NOT NULL)", srcFullName)))
	for i := 1; i <= numRows; i++ {
		require.NoError(s.t, s.source.Exec(s.t.Context(),
			fmt.Sprintf("INSERT INTO %s (id, val) VALUES ('key_%04d', %d)", srcFullName, i, i)))
	}

	tableMappings := TableMappings(s, srcTable, dstTable)
	// a String column can't be used directly as a Distributed sharding key (ClickHouse
	// requires an integer-typed sharding expression), so hash it for the cluster suite
	for _, tm := range tableMappings {
		tm.ShardingKey = "cityHash64(id)"
	}
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName: s.attachSuffix("string_pk_non_uuid"),
		// PartitionKey is not specified to test default partition key detection
		TableMappings: tableMappings,
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	flowConnConfig.SnapshotMaxParallelWorkers = 4
	flowConnConfig.SnapshotNumPartitionsOverride = numPartitions

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitForEqualTablesWithNames(env, s, "waiting on initial", srcTable, dstTable, "id,val")

	partitionRows, err := s.catalog.Query(s.t.Context(),
		`SELECT rows_in_partition FROM peerdb_stats.qrep_partitions WHERE parent_mirror_name = $1`,
		flowConnConfig.FlowJobName)
	require.NoError(s.t, err)
	defer partitionRows.Close()

	var partitionCount int32
	var totalRows int64
	for partitionRows.Next() {
		var rowsInPartition int64
		require.NoError(s.t, partitionRows.Scan(&rowsInPartition))
		totalRows += rowsInPartition
		partitionCount++
	}
	require.NoError(s.t, partitionRows.Err())
	require.EqualValues(s.t, 1, partitionCount)
	require.EqualValues(s.t, numRows, totalRows)

	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
