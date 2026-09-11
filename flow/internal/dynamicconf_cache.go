package internal

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CachedDynconfSetting caches the value returned by a typed dynamic setting getter.
type CachedDynconfSetting[T any] struct {
	loadedAt time.Time
	value    T
	getter   func(context.Context, map[string]string, string) (T, error)
	name     string
	ttl      time.Duration
	once     sync.Once
	mu       sync.Mutex
}

// InitOnce configures the setting cache on its first call.
func (s *CachedDynconfSetting[T]) InitOnce(
	name string,
	ttl time.Duration,
	getter func(context.Context, map[string]string, string) (T, error),
) {
	s.once.Do(func() {
		s.name = name
		s.ttl = ttl
		s.getter = getter
	})
}

// Get returns the cached value or refreshes it using the configured getter.
func (s *CachedDynconfSetting[T]) Get(ctx context.Context, env map[string]string) (T, error) {
	if s.getter == nil {
		var zero T
		return zero, errors.New("cached dynamic setting is not initialized")
	}

	if _, overridden := env[s.name]; overridden {
		return s.getter(ctx, env, s.name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.loadedAt.IsZero() && time.Since(s.loadedAt) < s.ttl {
		return s.value, nil
	}

	value, err := s.getter(ctx, nil, s.name)
	if err != nil {
		var zero T
		return zero, err
	}
	s.value = value
	s.loadedAt = time.Now()
	return value, nil
}
