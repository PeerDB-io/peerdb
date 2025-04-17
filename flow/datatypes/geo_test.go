package datatypes

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidHexWkb(t *testing.T) {
	_, err := GeoValidate("nothex")
	require.ErrorIs(t, err, hex.InvalidByteError('n'))

	_, err = GeoValidate("abc")
	require.ErrorIs(t, err, hex.ErrLength)
}

func TestInvalidWkt(t *testing.T) {
	_, err := GeoToWKB("invalid")
	require.Error(t, err)
}
