package shared

import (
	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	selector workflow.Selector
	ferror   error
	limit    int
	count    int
}

func NewBoundSelector(ctx workflow.Context, selectorName string, limit int) *BoundSelector {
	return &BoundSelector{
		limit:    limit,
		selector: workflow.NewNamedSelector(ctx, selectorName),
	}
}

// SpawnChild starts a child workflow, waiting first if the limit is reached. onSuccess, if set, runs
// once the child completes without error.
func (s *BoundSelector) SpawnChild(ctx workflow.Context, w any, onSuccess func(), args ...any) error {
	if s.ferror != nil {
		return s.ferror
	}
	if s.limit > 0 && s.count >= s.limit {
		s.waitOne(ctx)
		if s.ferror != nil {
			return s.ferror
		}
	}

	future := workflow.ExecuteChildWorkflow(ctx, w, args...)
	s.selector.AddFuture(future, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			s.ferror = err
		} else if onSuccess != nil {
			onSuccess()
		}
	})
	s.count += 1
	return s.ferror
}

func (s *BoundSelector) Wait(ctx workflow.Context) error {
	for s.count > 0 && s.ferror == nil {
		s.waitOne(ctx)
	}

	return s.ferror
}

// WaitForCancellation drains every outstanding child, even after the first cancellation error.
func (s *BoundSelector) WaitForCancellation(ctx workflow.Context) {
	if ctx.Err() == nil {
		return
	}
	waitCtx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	for s.count > 0 {
		s.waitOne(waitCtx)
	}
}

func (s *BoundSelector) waitOne(ctx workflow.Context) {
	if s.count > 0 {
		s.selector.Select(ctx)
		s.count -= 1
	}
}
