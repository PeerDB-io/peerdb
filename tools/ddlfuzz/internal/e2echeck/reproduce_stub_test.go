//go:build !ddlfuzz

package e2echeck

import "testing"

func TestReproduceRequiresDdlfuzzBuild(t *testing.T) {
	t.Skip("e2echeck.Reproduce exercises the ddlfuzz parser shim; run with -tags ddlfuzz")
}
