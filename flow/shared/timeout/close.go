package timeout

import "time"

// CloseWithTimeout runs closer in a goroutine and waits up to timeout for it
// to return. If it doesn't, onTimeout is invoked and CloseWithTimeout returns
// without waiting further.
func CloseWithTimeout(closer func(), timeout time.Duration, onTimeout func()) {
	done := make(chan struct{})
	go func() {
		closer()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		onTimeout()
	}
}
