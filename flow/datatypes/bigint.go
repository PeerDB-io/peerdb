package datatypes

import (
	"math/big"
)

func CountDigits(bi *big.Int) int {
	if bi.Sign() < 0 {
		return len(bi.String()) - 1
	}
	return len(bi.String())
}
