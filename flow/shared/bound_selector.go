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

func NewBoundSelector(ctx workflow.Context, selectorName string, limit int) *BoundSelector {
	return &BoundSelector{
		limit:    limit,
		selector: workflow.NewNamedSelector(ctx, selectorName),
	}
}

func (s *BoundSelector) SpawnChild(ctx workflow.Context, w any, futureCallback func(workflow.Future), args ...any) {
	if s.limit > 0 && s.count >= s.limit {
		s.waitOne(ctx)
	}

	future := workflow.ExecuteChildWorkflow(ctx, w, args...)
	if futureCallback != nil {
		s.selector.AddFuture(future, futureCallback)
	} else {
		s.selector.AddFuture(future, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				s.ferrors = append(s.ferrors, err)
			}
		})
	}
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
