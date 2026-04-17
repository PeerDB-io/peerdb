package connmysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestParseReplicationOffsetText(t *testing.T) {
	t.Run("filepos", func(t *testing.T) {
		parsedOffset, err := parseReplicationOffsetText("mysql", "!f:mysql-bin.000001,4d2")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_FILEPOS.String(), parsedOffset.mechanism)
		require.Nil(t, parsedOffset.gset)
		require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 0x4d2}, parsedOffset.pos)
	})

	t.Run("filepos missing comma", func(t *testing.T) {
		_, err := parseReplicationOffsetText("mysql", "!f:mysql-bin.000001")
		require.ErrorContains(t, err, "no comma in file/pos offset")
	})

	t.Run("filepos invalid offset", func(t *testing.T) {
		_, err := parseReplicationOffsetText("mysql", "!f:mysql-bin.000001,zzz")
		require.ErrorContains(t, err, "invalid offset in file/pos offset")
	})

	t.Run("gtid", func(t *testing.T) {
		parsedOffset, err := parseReplicationOffsetText("mysql", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_GTID.String(), parsedOffset.mechanism)
		require.Equal(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:23", parsedOffset.gset.String())
		require.Equal(t, mysql.Position{}, parsedOffset.pos)
	})

	t.Run("empty", func(t *testing.T) {
		parsedOffset, err := parseReplicationOffsetText("mysql", "")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_GTID.String(), parsedOffset.mechanism)
		require.True(t, parsedOffset.gset.IsEmpty())
		require.Equal(t, mysql.Position{}, parsedOffset.pos)
	})

	t.Run("invalid gtid", func(t *testing.T) {
		_, err := parseReplicationOffsetText("mysql", "not-a-valid-offset")
		require.ErrorContains(t, err, `failed to parse mysql offset text "not-a-valid-offset" as GTID set`)
	})
}

func TestReplicationMechanismInUseFromOffsetText(t *testing.T) {
	t.Run("filepos", func(t *testing.T) {
		mechanism, err := replicationMechanismInUseFromOffsetText("mysql", "!f:mysql-bin.000001,4d2")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_FILEPOS.String(), mechanism)
	})

	t.Run("gtid", func(t *testing.T) {
		mechanism, err := replicationMechanismInUseFromOffsetText("mysql", "3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_GTID.String(), mechanism)
	})

	t.Run("empty", func(t *testing.T) {
		mechanism, err := replicationMechanismInUseFromOffsetText("mysql", "")
		require.NoError(t, err)
		require.Equal(t, protos.MySqlReplicationMechanism_MYSQL_GTID.String(), mechanism)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := replicationMechanismInUseFromOffsetText("mysql", "not-a-valid-offset")
		require.ErrorContains(t, err, `failed to parse mysql offset text "not-a-valid-offset" as GTID set`)
	})
}
