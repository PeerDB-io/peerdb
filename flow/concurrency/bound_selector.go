package concurrency

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

type BoundSelector struct {
	ctx        workflow.Context
	limit      workflow.Channel
	statusCh   workflow.Channel
	numFutures int
}

func NewBoundSelector(limit int, total int, ctx workflow.Context) *BoundSelector {
	return &BoundSelector{
		ctx:        ctx,
		limit:      workflow.NewBufferedChannel(ctx, limit),
		statusCh:   workflow.NewBufferedChannel(ctx, total),
		numFutures: 0,
	}
}

func (s *BoundSelector) SpawnChild(chCtx workflow.Context, w interface{}, args ...interface{}) {
	s.numFutures++
	workflow.Go(s.ctx, func(ctx workflow.Context) {
		s.limit.Send(ctx, struct{}{})
		future := workflow.ExecuteChildWorkflow(chCtx, w, args...)
		err := future.Get(ctx, nil)
		s.statusCh.Send(ctx, err)
		s.limit.Receive(ctx, nil)
	})
}

func (s *BoundSelector) Wait() error {
	defer s.statusCh.Close()
	defer s.limit.Close()

	ferrors := make([]error, 0)
	doneCount := 0

	for doneCount < s.numFutures {
		selector := workflow.NewSelector(s.ctx)
		selector.AddReceive(s.statusCh, func(c workflow.ReceiveChannel, more bool) {
			var err error
			c.Receive(s.ctx, &err)
			if err != nil {
				ferrors = append(ferrors, err)
			}
			doneCount++
		})
		selector.Select(s.ctx)
	}

	if len(ferrors) > 0 {
		return errors.Join(ferrors...)
	}

	return nil
}
