package connmysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestQkindFromMysqlColumnTypeRejectsCompressedColumns(t *testing.T) {
	testCases := []string{
		"varchar(100) /*M!100301 COMPRESSED*/",
		"text /*M!100301 COMPRESSED*/",
	}

	for _, columnType := range testCases {
		t.Run(columnType, func(t *testing.T) {
			_, err := QkindFromMysqlColumnType(columnType, true, shared.InternalVersion_Latest)
			require.Error(t, err)
			require.Contains(t, err.Error(), "COMPRESSED")
			require.Contains(t, err.Error(), "unsupported")
		})
	}
}

func TestQkindFromMysqlColumnTypeDoesNotRejectCompressedEnumLabel(t *testing.T) {
	qkind, err := QkindFromMysqlColumnType("enum('compressed','plain')", true, shared.InternalVersion_Latest)
	require.NoError(t, err)
	require.Equal(t, types.QValueKindEnum, qkind)
}
