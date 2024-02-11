package concurrency

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	limit   int
	futures []workflow.Future
	ferrors []error
}

func NewBoundSelector(limit int) *BoundSelector {
	return &BoundSelector{
		limit: limit,
	}
}

func (s *BoundSelector) SpawnChild(chCtx workflow.Context, w interface{}, args ...interface{}) {
	if len(s.futures) >= s.limit {
		s.waitOne(chCtx)
	}

	future := workflow.ExecuteChildWorkflow(chCtx, w, args...)
	s.futures = append(s.futures, future)
}

func (s *BoundSelector) waitOne(ctx workflow.Context) {
	if len(s.futures) == 0 {
		return
	}

	f := s.futures[0]
	s.futures = s.futures[1:]

	err := f.Get(ctx, nil)
	if err != nil {
		s.ferrors = append(s.ferrors, err)
	}
}

func (s *BoundSelector) Wait(ctx workflow.Context) error {
	for len(s.futures) > 0 {
		s.waitOne(ctx)
	}

	if len(s.ferrors) > 0 {
		return errors.Join(s.ferrors...)
	}

	return nil
}
