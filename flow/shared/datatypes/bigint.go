package datatypes

import (
	"math"
	"math/big"
)

var tenInt = big.NewInt(10)

func CountDigits(bi *big.Int) int {
	if bi.IsInt64() {
		i64 := bi.Int64()
		// restrict fast path to integers with exact conversion to float64
		if i64 <= (1<<53) && i64 >= -(1<<53) {
			if i64 == 0 {
				return 1
			}
			return int(math.Log10(math.Abs(float64(i64)))) + 1
		}
	}

	estimatedNumDigits := int(float64(bi.BitLen()) / math.Log2(10))

	// estimatedNumDigits (lg10) may be off by 1, need to verify
	digitsBigInt := big.NewInt(int64(estimatedNumDigits))
	errorCorrectionUnit := digitsBigInt.Exp(tenInt, digitsBigInt, nil)

	if bi.CmpAbs(errorCorrectionUnit) >= 0 {
		return estimatedNumDigits + 1
	}

	return estimatedNumDigits
}
