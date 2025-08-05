package datatypes

import (
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountDigits(t *testing.T) {
	require.Equal(t, 1, CountDigits(big.NewInt(-1)))
	require.Equal(t, 1, CountDigits(big.NewInt(0)))
	require.Equal(t, 18, CountDigits(big.NewInt(999999999999999999)))
	require.Equal(t, 18, CountDigits(big.NewInt(-999999999999999999)))
	// this relies on correction
	bi := new(big.Int)
	bi.SetString("1"+strings.Repeat("0", 999), 10)
	require.Equal(t, 1000, CountDigits(bi))
}
