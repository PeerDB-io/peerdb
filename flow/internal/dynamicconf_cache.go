package internal

import (
	"context"
	"sync"
	"time"
)

// CachedDynconfSetting caches the value returned by a typed dynamic setting getter.
type CachedDynconfSetting[T any] struct {
	getter   func(context.Context, map[string]string) (T, error)
	ttl      time.Duration
	mu       sync.Mutex
	value    T
	loadedAt time.Time
}

// NewCachedDynconfSetting creates a typed dynamic setting cache with the given lifetime.
func NewCachedDynconfSetting[T any](
	getter func(context.Context, map[string]string) (T, error),
	ttl time.Duration,
) *CachedDynconfSetting[T] {
	return &CachedDynconfSetting[T]{
		getter: getter,
		ttl:    ttl,
	}
}

// Get returns the cached value or refreshes it using the configured getter.
func (s *CachedDynconfSetting[T]) Get(ctx context.Context, env map[string]string) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.loadedAt.IsZero() && time.Since(s.loadedAt) < s.ttl {
		return s.value, nil
	}

	value, err := s.getter(ctx, env)
	if err != nil {
		var zero T
		return zero, err
	}
	s.value = value
	s.loadedAt = time.Now()
	return value, nil
}
