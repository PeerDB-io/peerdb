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
	if ch := lc.cond.Swap(&newch); ch != nil {
		close(*ch)
	}
}

func (lc *LastChan) Close() {
	if ch := lc.cond.Swap(nil); ch != nil {
		close(*ch)
	}
}

func (lc *LastChan) Wait() <-chan struct{} {
	if ch := lc.cond.Load(); ch != nil {
		return *ch
	}
	return nil
}

func (lc *LastChan) Load() int64 {
	return lc.val.Load()
}
