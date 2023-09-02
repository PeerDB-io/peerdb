package concurrency

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	ctx     workflow.Context
	limit   int
	futures []workflow.Future
	ferrors []error
}

func NewBoundSelector(limit int, total int, ctx workflow.Context) *BoundSelector {
	return &BoundSelector{
		ctx:   ctx,
		limit: limit,
	}
}

func (s *BoundSelector) SpawnChild(chCtx workflow.Context, w interface{}, args ...interface{}) {
	if len(s.futures) >= s.limit {
		s.waitOne()
	}

	future := workflow.ExecuteChildWorkflow(chCtx, w, args...)
	s.futures = append(s.futures, future)
}

func (s *BoundSelector) waitOne() {
	if len(s.futures) == 0 {
		return
	}

	f := s.futures[0]
	s.futures = s.futures[1:]

	err := f.Get(s.ctx, nil)
	if err != nil {
		s.ferrors = append(s.ferrors, err)
	}
}

func (s *BoundSelector) Wait() error {
	for len(s.futures) > 0 {
		s.waitOne()
	}

	if len(s.ferrors) > 0 {
		return errors.Join(s.ferrors...)
	}

	return nil
}
