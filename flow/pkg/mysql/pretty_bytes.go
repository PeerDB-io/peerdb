package mysql

import (
	"fmt"
	"math"
)

// based on postgres pg_size_pretty
type prettyUnit struct {
	name  string
	limit int64
}

var prettyUnits = []prettyUnit{
	{"bytes", 10 * 1024},
	{"kB", 20*1024 - 1},
	{"MB", 20*1024 - 1},
	{"GB", 20*1024 - 1},
	{"TB", 20*1024 - 1},
	{"PB", math.MaxInt64},
}

func PrettyBytes(size int64) string {
	for idx, unit := range prettyUnits {
		if size < unit.limit {
			if idx > 0 {
				size = (size + 1) / 2
			}

			return fmt.Sprintf("%d %s", size, unit.name)
		}

		size >>= 9
		if idx > 0 {
			size >>= 1
		}
	}
	// should never reach here
	return fmt.Sprintf("%d bytes", size)
}
