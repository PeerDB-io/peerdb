//go:build !ddlfuzz

package e2echeck

import "fmt"

func Reproduce(in Input) (Result, error) {
	return Result{}, fmt.Errorf("e2e replay for class %q requires build with -tags ddlfuzz", in.Class)
}
