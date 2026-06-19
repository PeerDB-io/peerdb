package connmysql

import (
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func newMasterStatusResult(rows ...[]mysql.FieldValue) *mysql.Result {
	rs := mysql.NewResultset(2)
	rs.Fields[0] = &mysql.Field{Name: []byte("File")}
	rs.Fields[1] = &mysql.Field{Name: []byte("Position")}
	rs.Values = rows
	return mysql.NewResult(rs)
}

func mysqlStringValue(value string) mysql.FieldValue {
	return mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(value))
}

func mysqlUintValue(value uint64) mysql.FieldValue {
	return mysql.NewFieldValue(mysql.FieldValueTypeUnsigned, value, nil)
}

func TestDecodeMasterPosResult(t *testing.T) {
	const showBinlogStatus = "SHOW BINARY LOG STATUS"

	t.Run("valid", func(t *testing.T) {
		pos, err := decodeMasterPosResult(newMasterStatusResult([]mysql.FieldValue{
			mysqlStringValue("mysql-bin.000001"),
			mysqlUintValue(1234),
		}), showBinlogStatus)

		require.NoError(t, err)
		require.Equal(t, mysql.Position{Name: "mysql-bin.000001", Pos: 1234}, pos)
	})

	t.Run("empty result", func(t *testing.T) {
		_, err := decodeMasterPosResult(newMasterStatusResult(), showBinlogStatus)

		require.ErrorContains(t, err, "no binary log position")
		require.ErrorContains(t, err, "binary logging may be disabled")
	})

	t.Run("missing position", func(t *testing.T) {
		_, err := decodeMasterPosResult(newMasterStatusResult([]mysql.FieldValue{
			mysqlStringValue(""),
			mysqlUintValue(0),
		}), showBinlogStatus)

		require.ErrorContains(t, err, "no binary log position")
		require.ErrorContains(t, err, "position is unreadable")
	})

	t.Run("position exceeds uint32", func(t *testing.T) {
		_, err := decodeMasterPosResult(newMasterStatusResult([]mysql.FieldValue{
			mysqlStringValue("mysql-bin.000123"),
			mysqlUintValue(uint64(math.MaxUint32) + 1),
		}), showBinlogStatus)

		require.ErrorContains(t, err, "4GiB")
		require.ErrorContains(t, err, "GTID")
	})

	t.Run("log file decode error", func(t *testing.T) {
		rs := mysql.NewResultset(0)
		rs.Values = [][]mysql.FieldValue{{}}

		_, err := decodeMasterPosResult(mysql.NewResult(rs), showBinlogStatus)

		require.ErrorContains(t, err, "failed to read log file")
		require.ErrorContains(t, err, "invalid column index")
	})

	t.Run("position decode error", func(t *testing.T) {
		_, err := decodeMasterPosResult(newMasterStatusResult([]mysql.FieldValue{
			mysqlStringValue("mysql-bin.000001"),
			mysqlStringValue("not-a-number"),
		}), showBinlogStatus)

		require.ErrorContains(t, err, "failed to read log position")
		require.ErrorContains(t, err, "invalid syntax")
	})
}
