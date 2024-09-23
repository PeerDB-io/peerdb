package datatypes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

var parsingTypmodTests = map[int32]NumericTypmod{
	-1:       {constrained: false},
	0:        {constrained: false},
	3:        {constrained: false},
	65540:    {constrained: true, precision: 1, scale: 0},
	65541:    {constrained: true, precision: 1, scale: 1},
	66588:    {constrained: true, precision: 1, scale: -1000},
	65536004: {constrained: true, precision: 1000, scale: 0},
	65537004: {constrained: true, precision: 1000, scale: 1000},
	65537052: {constrained: true, precision: 1000, scale: -1000},
	65538051: {constrained: true, precision: 1000, scale: -1},
	// precision 1001 onwards should trigger unconstrained
	65601540: {constrained: false},
}

func TestNewParsedNumericTypmod(t *testing.T) {
	for typmod, expected := range parsingTypmodTests {
		parsed := NewParsedNumericTypmod(typmod)
		assert.Equal(t, expected, *parsed)
	}
}

func TestParsedNumericTypmod_ToTypmod(t *testing.T) {
	for expected, parsed := range parsingTypmodTests {
		typmod := parsed.ToTypmod()
		if !parsed.constrained {
			assert.Equal(t, int32(-1), typmod)
			continue
		}
		assert.Equal(t, expected, typmod)
	}
}

func TestParsedNumericTypmod_ToDWHNumericConstraints(t *testing.T) {
	for _, parsed := range parsingTypmodTests {
		for _, dwh := range []protos.DBType{
			protos.DBType_BIGQUERY,
			protos.DBType_SNOWFLAKE,
			protos.DBType_CLICKHOUSE,
		} {
			precision, scale := parsed.ToDWHNumericConstraints(dwh)
			assert.LessOrEqual(t, precision, getMaxPrecisionForDWH(dwh))
			assert.LessOrEqual(t, scale, getMaxScaleForDWH(dwh))
			assert.Positive(t, precision)
			assert.GreaterOrEqual(t, scale, int16(0))
			assert.LessOrEqual(t, scale, precision)
		}
	}
}
