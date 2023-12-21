package e2eshared

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ysmood/got"
)

func GotSuite[T interface{ TearDownSuite() }](setup func(t *testing.T, g got.G) T) func(t *testing.T) T {
	return func(t *testing.T) T {
		g := got.New(t)
		g.Parallel()

		suite := setup(t, g)
		g.Cleanup(func() {
			suite.TearDownSuite()
		})

		return suite
	}
}

// ReadFileToBytes reads a file to a byte array.
func ReadFileToBytes(path string) ([]byte, error) {
	var ret []byte

	f, err := os.Open(path)
	if err != nil {
		return ret, fmt.Errorf("failed to open file: %w", err)
	}

	defer f.Close()

	ret, err = io.ReadAll(f)
	if err != nil {
		return ret, fmt.Errorf("failed to read file: %w", err)
	}

	return ret, nil
}
