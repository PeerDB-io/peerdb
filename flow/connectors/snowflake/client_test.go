package connsnowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestSnowflakeSchemaTableNormalize(t *testing.T) {
	// dotless components keep the pre-QualifiedTable behavior byte for byte
	assert.Equal(t, `"PUBLIC"."MY_TABLE"`,
		snowflakeSchemaTableNormalize(common.QualifiedTable{Namespace: "public", Table: "my_table"}))
	assert.Equal(t, `"MySchema"."MyTable"`,
		snowflakeSchemaTableNormalize(common.QualifiedTable{Namespace: "MySchema", Table: "MyTable"}))
	// dots stay inside the quoted component
	assert.Equal(t, `"SCH.EMA"."TA.BLE"`,
		snowflakeSchemaTableNormalize(common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}))
}
