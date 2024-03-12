package shared

import "golang.org/x/exp/constraints"

func DivCeil[T constraints.Integer](x, y T) T {
	return (x + y - 1) / y
}
