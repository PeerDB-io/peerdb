package e2eshared

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ysmood/got"
)

func GotSuite[T any](t *testing.T, setup func(t *testing.T) T, teardown func(T)) {
	t.Helper()

	got.Each(t, func(t *testing.T) T {
		t.Helper()
		t.Parallel()
		suite := setup(t)
		t.Cleanup(func() {
			teardown(suite)
		})
		return suite
	})
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
