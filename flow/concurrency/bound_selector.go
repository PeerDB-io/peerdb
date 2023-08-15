package concurrency

import (
	"errors"
	"sync"

	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	ctx      workflow.Context
	limit    int
	selector workflow.Selector
	futures  map[workflow.Future]struct{}
	ferrors  []error
	mu       sync.Mutex
}

func NewBoundSelector(limit int, ctx workflow.Context) *BoundSelector {
	return &BoundSelector{
		ctx:      ctx,
		limit:    limit,
		selector: workflow.NewSelector(ctx),
		futures:  make(map[workflow.Future]struct{}),
		ferrors:  make([]error, 0),
	}
}

func (s *BoundSelector) AddFuture(future workflow.Future, f func(workflow.Future) error) {
	s.mu.Lock()
	if len(s.futures) >= s.limit {
		s.selector.Select(s.ctx)
	}
	s.futures[future] = struct{}{}
	s.mu.Unlock()

	s.selector.AddFuture(future, func(ready workflow.Future) {
		s.mu.Lock()
		delete(s.futures, ready)

		err := f(ready)
		if err != nil {
			s.ferrors = append(s.ferrors, err)
		}
		s.mu.Unlock()
	})
}

func (s *BoundSelector) Wait() error {
	for {
		s.mu.Lock()
		if len(s.futures) == 0 {
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		s.selector.Select(s.ctx)
	}

	if len(s.ferrors) > 0 {
		return errors.Join(s.ferrors...)
	}

	return nil
}
