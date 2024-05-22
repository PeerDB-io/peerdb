package datatypes

import (
	"math/big"
	"testing"
)

func TestCountDigits(t *testing.T) {
	bi := big.NewInt(1234567890)
	expected := 10
	result := CountDigits(bi)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}

	bi = big.NewInt(-1234567890)
	expected = 10
	result = CountDigits(bi)
	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}

	// 18 nines
	bi = big.NewInt(999999999999999999)
	result = CountDigits(bi)
	expected = 18
	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}

	// 18 nines
	bi = big.NewInt(-999999999999999999)
	result = CountDigits(bi)
	expected = 18
	if result != expected {
		t.Errorf("Unexpected result. Expected: %v, but got: %v", expected, result)
	}
}
