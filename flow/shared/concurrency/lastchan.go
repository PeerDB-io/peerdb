package concurrency

import (
	"sync"
	"sync/atomic"
)

type LastChan struct {
	cond sync.Cond
	val  atomic.Int64
}

func NewLastChan() *LastChan {
	return &LastChan{cond: sync.Cond{L: &sync.Mutex{}}}
}

func (lc *LastChan) Update(val int64) {
	lc.val.Store(val)
	lc.cond.Broadcast()
}

func (lc *LastChan) Broadcast() {
	lc.cond.Broadcast()
}

func (lc *LastChan) MaybeWait(old int64) int64 {
	if val := lc.val.Load(); val != old {
		return val
	}
	return lc.Wait()
}

func (lc *LastChan) Wait() int64 {
	lc.cond.Wait()
	return lc.val.Load()
}

func (lc *LastChan) Load() int64 {
	return lc.val.Load()
}
