package connmysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// parseOffsetTestCase describes the validation case, separating gtid checks from fpos checks
// ussing `offsetExpecation` interface.
type parseOffsetTestCase struct {
	name       string
	flavor     string
	offsetText string

	// If non-empty, parsing is expected to fail and the error must contain this substring.
	maybeErrContains string

	expect offsetExpectation
}

// offsetExpectation owns the mechanism-specific success assertions> gtid vs fpos.
type offsetExpectation interface {
	// wantMechanism is the mechanism a successful parse must resolve to.
	wantMechanism() protos.MySqlReplicationMechanism
	// verifyParse asserts the parsed payload.
	verifyParse(t *testing.T, parsed parsedReplicationOffset)
}

type gtidExpectation struct {
	gtidString string
	gtidEmpty  bool
}

func (gtidExpectation) wantMechanism() protos.MySqlReplicationMechanism {
	return protos.MySqlReplicationMechanism_MYSQL_GTID
}

func (e gtidExpectation) verifyParse(t *testing.T, parsed parsedReplicationOffset) {
	t.Helper()
	require.Equal(t, e.wantMechanism().String(), parsed.mechanism)
	require.Equal(t, mysql.Position{}, parsed.pos)
	require.NotNil(t, parsed.gset)
	require.Equal(t, e.gtidString, parsed.gset.String())
	if e.gtidEmpty {
		require.True(t, parsed.gset.IsEmpty())
	}
}

type fposExpectation struct {
	pos mysql.Position
}

func (fposExpectation) wantMechanism() protos.MySqlReplicationMechanism {
	return protos.MySqlReplicationMechanism_MYSQL_FILEPOS
}

func (e fposExpectation) verifyParse(t *testing.T, parsed parsedReplicationOffset) {
	t.Helper()
	require.Equal(t, e.wantMechanism().String(), parsed.mechanism)
	require.Nil(t, parsed.gset)
	require.Equal(t, e.pos, parsed.pos)
}

// parseOffsetTestCases drives both TestParseReplicationOffsetText and
// TestReplicationMechanismInUseFromOffsetText.
var parseOffsetTestCases = []parseOffsetTestCase{
	{
		name:       "mysql filepos",
		flavor:     mysql.MySQLFlavor,
		offsetText: "!f:mysql-bin.000001,4d2",
		expect:     fposExpectation{pos: mysql.Position{Name: "mysql-bin.000001", Pos: 0x4d2}},
	},
	{
		name:             "mysql filepos missing comma",
		flavor:           mysql.MySQLFlavor,
		offsetText:       "!f:mysql-bin.000001",
		maybeErrContains: "no comma in file/pos offset",
	},
	{
		name:             "mysql filepos invalid offset",
		flavor:           mysql.MySQLFlavor,
		offsetText:       "!f:mysql-bin.000001,zzz",
		maybeErrContains: "invalid offset in file/pos offset",
	},
	{
		name:       "mysql gtid single uuid",
		flavor:     mysql.MySQLFlavor,
		offsetText: "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
		expect:     gtidExpectation{gtidString: "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"},
	},
	{
		name:       "mysql empty",
		flavor:     mysql.MySQLFlavor,
		offsetText: "",
		expect:     gtidExpectation{gtidEmpty: true},
	},
	{
		name:             "mysql invalid gtid",
		flavor:           mysql.MySQLFlavor,
		offsetText:       "not-a-valid-offset",
		maybeErrContains: `failed to parse mysql offset text "not-a-valid-offset" as GTID set`,
	},
	{
		name:       "mariadb gtid single domain",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "0-1-23",
		expect:     gtidExpectation{gtidString: "0-1-23"},
	},
	{
		name:       "mariadb gtid non-default domain and multi-digit fields",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "5-100-9999999",
		expect:     gtidExpectation{gtidString: "5-100-9999999"},
	},
	{
		name:       "mariadb gtid max uint64 sequence",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "5-100-18446744073709551615",
		expect:     gtidExpectation{gtidString: "5-100-18446744073709551615"},
	},
	{
		name:       "mariadb gtid multi-domain already sorted",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "0-1-23,1-2-45",
		expect:     gtidExpectation{gtidString: "0-1-23,1-2-45"},
	},
	{
		name:       "mariadb gtid multi-domain canonicalizes order",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "1-2-45,0-1-23",
		expect:     gtidExpectation{gtidString: "0-1-23,1-2-45"},
	},
	{
		// A MariaDB position is one GTID per domain: the same domain twice collapses
		// to the newest sequence.
		name:       "mariadb gtid same domain collapses to newest sequence",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "0-1-23,0-1-50",
		expect:     gtidExpectation{gtidString: "0-1-50"},
	},
	{
		// Failover within a domain: server_id changes, the newest entry wins.
		name:       "mariadb gtid same domain failover keeps newest server",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "0-1-23,0-2-50",
		expect:     gtidExpectation{gtidString: "0-2-50"},
	},
	{
		name:       "mariadb empty",
		flavor:     mysql.MariaDBFlavor,
		offsetText: "",
		expect:     gtidExpectation{gtidEmpty: true},
	},
	{
		// MySQL uuid:interval grammar must not parse under the MariaDB flavor.
		name:             "mariadb rejects mysql-style uuid gtid",
		flavor:           mysql.MariaDBFlavor,
		offsetText:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
		maybeErrContains: "must domain-server-sequence",
	},
	{
		name:             "mariadb gtid too few parts",
		flavor:           mysql.MariaDBFlavor,
		offsetText:       "0-1",
		maybeErrContains: "must domain-server-sequence",
	},
	{
		name:             "mariadb gtid non-numeric domain",
		flavor:           mysql.MariaDBFlavor,
		offsetText:       "a-1-23",
		maybeErrContains: "invalid MariaDB GTID Domain ID",
	},
}

func TestParseReplicationOffsetText(t *testing.T) {
	for _, tc := range parseOffsetTestCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseReplicationOffsetText(tc.flavor, tc.offsetText)
			if tc.maybeErrContains != "" {
				require.ErrorContains(t, err, tc.maybeErrContains)
				return
			}
			require.NoError(t, err)
			tc.expect.verifyParse(t, parsed)
		})
	}
}

func TestReplicationMechanismInUseFromOffsetText(t *testing.T) {
	for _, tc := range parseOffsetTestCases {
		t.Run(tc.name, func(t *testing.T) {
			mechanism, err := replicationMechanismInUseFromOffsetText(tc.flavor, tc.offsetText)
			if tc.maybeErrContains != "" {
				require.ErrorContains(t, err, tc.maybeErrContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expect.wantMechanism().String(), mechanism)
		})
	}
}
