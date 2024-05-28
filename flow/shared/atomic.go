package shared

import "sync/atomic"

// TODO remove after https://github.com/golang/go/issues/63999
func AtomicInt64Max(a *atomic.Int64, v int64) {
	oldLast := a.Load()
	for oldLast < v && !a.CompareAndSwap(oldLast, v) {
		oldLast = a.Load()
	}
}
