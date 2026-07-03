//go:build !ddlfuzz

package exec

import "fmt"

func DefaultParser(sql []byte, sqlMode uint64, isMariaDB bool) (string, error) {
	return "", fmt.Errorf("ddlfuzz parser shim unavailable: build with -tags ddlfuzz")
}
