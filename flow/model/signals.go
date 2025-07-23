package model

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// typed wrapper around temporal signals

type TypedSignal[T any] struct {
	Name string
}

func (self TypedSignal[T]) GetSignalChannel(ctx workflow.Context) TypedReceiveChannel[T] {
	return TypedReceiveChannel[T]{
		Chan: workflow.GetSignalChannel(ctx, self.Name),
	}
}

func (self TypedSignal[T]) SignalClientWorkflow(
	ctx context.Context,
	c client.Client,
	workflowID string,
	runID string,
	value T,
) error {
	return c.SignalWorkflow(ctx, workflowID, runID, self.Name, value)
}

func (self TypedSignal[T]) SignalChildWorkflow(
	ctx workflow.Context,
	wf workflow.ChildWorkflowFuture,
	value T,
) workflow.Future {
	return wf.SignalChildWorkflow(ctx, self.Name, value)
}

func (self TypedSignal[T]) SignalExternalWorkflow(
	ctx workflow.Context,
	workflowID string,
	runID string,
	value T,
) workflow.Future {
	return workflow.SignalExternalWorkflow(ctx, workflowID, runID, self.Name, value)
}

type TypedReceiveChannel[T any] struct {
	Chan workflow.ReceiveChannel
}

func (self TypedReceiveChannel[T]) Receive(ctx workflow.Context) (T, bool) {
	var result T
	more := self.Chan.Receive(ctx, &result)
	return result, more
}

func (self TypedReceiveChannel[T]) ReceiveWithTimeout(ctx workflow.Context, timeout time.Duration) (T, bool, bool) {
	var result T
	ok, more := self.Chan.ReceiveWithTimeout(ctx, timeout, &result)
	return result, ok, more
}

func (self TypedReceiveChannel[T]) ReceiveAsync() (T, bool) {
	var result T
	ok := self.Chan.ReceiveAsync(&result)
	return result, ok
}

func (self TypedReceiveChannel[T]) ReceiveAsyncWithMoreFlag() (T, bool, bool) {
	var result T
	ok, more := self.Chan.ReceiveAsyncWithMoreFlag(&result)
	return result, ok, more
}

func (self TypedReceiveChannel[T]) AddToSelector(selector workflow.Selector, f func(T, bool)) workflow.Selector {
	return selector.AddReceive(self.Chan, func(c workflow.ReceiveChannel, more bool) {
		var result T
		if !c.ReceiveAsync(&result) {
			panic("AddReceive selector should not give empty channel")
		}
		f(result, more)
	})
}

type CDCFlowSignal int32

const (
	NoopSignal CDCFlowSignal = iota
	_
	PauseSignal
	TerminateSignal
	ResyncSignal
)

func FlowSignalHandler(activeSignal CDCFlowSignal,
	v CDCFlowSignal, logger log.Logger,
) CDCFlowSignal {
	switch v {
	case PauseSignal:
		logger.Info("received pause signal")
		if activeSignal == NoopSignal {
			logger.Info("workflow was running, pausing it")
			return v
		}
	case NoopSignal:
		logger.Info("received resume signal")
		if activeSignal == PauseSignal {
			logger.Info("workflow was paused, resuming it")
			return v
		}
	case TerminateSignal:
		return v
	case ResyncSignal:
		return v
	}
	return activeSignal
}

var FlowSignal = TypedSignal[CDCFlowSignal]{
	Name: "peer-flow-signal",
}

var FlowSignalStateChange = TypedSignal[*protos.FlowStateChangeRequest]{
	Name: "flow-state-change-signal",
}

var CDCDynamicPropertiesSignal = TypedSignal[*protos.CDCFlowConfigUpdate]{
	Name: "cdc-dynamic-properties",
}

var StartMaintenanceSignal = TypedSignal[*protos.StartMaintenanceSignal]{
	Name: "start-maintenance-signal",
}

func SleepFuture(ctx workflow.Context, d time.Duration) workflow.Future {
	f, set := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		set.Set(nil, workflow.Sleep(ctx, d))
	})
	return f
}
