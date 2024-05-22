package datatypes

import (
	"math"
	"math/big"
)

var tenInt = big.NewInt(10)

func CountDigits(bi *big.Int) int {
	tenInt := big.NewInt(10)
	if bi.IsUint64() {
		u64 := bi.Uint64()
		if u64 < (1 << 53) { // Check if it is less than 2^53
			if u64 == 0 {
				return 1 // 0 has one digit
			}
			return int(math.Log10(float64(u64))) + 1
		}
	} else if bi.IsInt64() {
		i64 := bi.Int64()
		if i64 > -(1 << 53) { // Check if it is greater than -2^53
			if i64 == 0 {
				return 1 // 0 has one digit
			}
			return int(math.Log10(float64(-i64))) + 1
		}
	}

	// For larger numbers, use the bit length and logarithms
	abs := new(big.Int).Abs(bi)
	lg10 := int(float64(abs.BitLen()) / math.Log2(10))

	// Verify and adjust lg10 if necessary
	check := new(big.Int).Exp(tenInt, big.NewInt(int64(lg10)), nil)
	if abs.Cmp(check) >= 0 {
		lg10++
	}

	return lg10
}
