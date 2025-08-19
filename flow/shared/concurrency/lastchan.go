package concurrency

import (
	"sync/atomic"
)

type LastChan struct {
	cond atomic.Pointer[chan struct{}]
	val  atomic.Int64
}

func NewLastChan() *LastChan {
	lc := &LastChan{}
	ch := make(chan struct{})
	lc.cond.Store(&ch)
	return lc
}

func (lc *LastChan) Update(val int64) {
	lc.val.Store(val)
	newch := make(chan struct{})
	ch := lc.cond.Swap(&newch)
	if ch != nil {
		close(*ch)
	}
}

func (lc *LastChan) Close() {
	ch := lc.cond.Swap(nil)
	if ch != nil {
		close(*ch)
	}
}

func (lc *LastChan) MaybeWait(old int64) (int64, bool) {
	if val := lc.val.Load(); val != old {
		return val, true
	}
	return lc.Wait()
}

func (lc *LastChan) Wait() (int64, bool) {
	ch := lc.cond.Load()
	if ch == nil {
		return 0, false
	}
	<-*ch
	return lc.val.Load(), true
}

func (lc *LastChan) Load() int64 {
	return lc.val.Load()
}
