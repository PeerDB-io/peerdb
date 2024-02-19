package peerflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// HeartbeatFlowWorkflow sends WAL heartbeats & monitors slot size
func HeartbeatFlowWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
	})

	// Use channels to time activities,
	// using two primes for frequency to scatter timing
	// After sending, next send should be delayed if activity takes longer than frequency
	doSlotSize := workflow.NewNamedChannel(ctx, "doSlotSize")
	doHeartbeat := workflow.NewNamedChannel(ctx, "doHeartbeat")
	doneSlotSize := workflow.NewNamedChannel(ctx, "doneSlotSize")
	doneHeartbeat := workflow.NewNamedChannel(ctx, "doneHeartbeat")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			doSlotSize.Send(ctx, nil)
			if workflow.Sleep(ctx, 5*time.Minute) != nil {
				return
			}
			doneSlotSize.Receive(ctx, nil)
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			doHeartbeat.Send(ctx, nil)
			if workflow.Sleep(ctx, 11*time.Minute) != nil {
				return
			}
			doneHeartbeat.Receive(ctx, nil)
		}
	})

	var canceled bool
	activities := 0
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(ctx.Done(), func(_ workflow.ReceiveChannel, _ bool) {
		canceled = true
	})
	selector.AddReceive(doSlotSize, func(c workflow.ReceiveChannel, _ bool) {
		if c.ReceiveAsync(nil) {
			slotSizeFuture := workflow.ExecuteActivity(ctx, flowable.RecordSlotSizes)
			selector.AddFuture(slotSizeFuture, func(f workflow.Future) {
				doneSlotSize.Send(ctx, nil)
			})
		}
	})
	selector.AddReceive(doHeartbeat, func(c workflow.ReceiveChannel, _ bool) {
		if c.ReceiveAsync(nil) {
			heartbeatFuture := workflow.ExecuteActivity(ctx, flowable.SendWALHeartbeat)
			selector.AddFuture(heartbeatFuture, func(f workflow.Future) {
				doneHeartbeat.Send(ctx, nil)
			})
		}
	})
	for {
		selector.Select(ctx)
		if canceled {
			return nil
		}

		activities += 1
		if activities > 99 {
			return workflow.NewContinueAsNewError(ctx, HeartbeatFlowWorkflow)
		}
	}
}
