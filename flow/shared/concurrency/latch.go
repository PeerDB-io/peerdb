package concurrency

import "sync"

// Latch is a thread-safe value holder that can be set once and read many times.
// Once Set is called, all future Wait calls will immediately return the set value.
type Latch[T any] struct {
	val   T
	ready chan struct{}
	once  sync.Once
}

// NewLatch creates a new Latch for type T.
func NewLatch[T any]() *Latch[T] {
	return &Latch[T]{ready: make(chan struct{})}
}

// Set publishes the value exactly once; later calls are no-ops.
func (l *Latch[T]) Set(v T) {
	l.once.Do(func() {
		l.val = v
		close(l.ready) // broadcasts to all waiters
	})
}

// Wait blocks until Set is called, then returns the value.
// Safe to call many times; subsequent calls return immediately.
func (l *Latch[T]) Wait() T {
	<-l.ready
	return l.val
}

// Chan returns a channel that will be closed when the value is set.
// Useful for select statements.
func (l *Latch[T]) Chan() <-chan struct{} {
	return l.ready
}

// IsSet returns true if the value has been set.
func (l *Latch[T]) IsSet() bool {
	select {
	case <-l.ready:
		return true
	default:
		return false
	}
}
