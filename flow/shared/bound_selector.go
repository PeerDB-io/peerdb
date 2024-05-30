package shared

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	selector workflow.Selector
	ferrors  []error
	limit    int
	count    int
}

func NewBoundSelector(ctx workflow.Context, limit int) *BoundSelector {
	return &BoundSelector{
		limit:    limit,
		selector: workflow.NewSelector(ctx),
	}
}

func (s *BoundSelector) SpawnChild(ctx workflow.Context, w interface{}, args ...interface{}) {
	if s.count >= s.limit {
		s.waitOne(ctx)
	}

	future := workflow.ExecuteChildWorkflow(ctx, w, args...)
	s.selector.AddFuture(future, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			s.ferrors = append(s.ferrors, err)
		}
	})
	s.count += 1
}

func (s *BoundSelector) waitOne(ctx workflow.Context) {
	if s.count > 0 {
		s.selector.Select(ctx)
		s.count -= 1
	}
}

func (s *BoundSelector) Wait(ctx workflow.Context) error {
	for s.count > 0 {
		s.waitOne(ctx)
	}

	return errors.Join(s.ferrors...)
}
