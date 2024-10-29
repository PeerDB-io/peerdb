package cmd

import (
	"context"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

type LoggedWorkflowInboundInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
	Next interceptor.WorkflowInboundInterceptor
}

func NewLoggedWorkflowInboundInterceptor(next interceptor.WorkflowInboundInterceptor) *LoggedWorkflowInboundInterceptor {
	return &LoggedWorkflowInboundInterceptor{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		Next:                           next,
	}
}

func (w *LoggedWorkflowInboundInterceptor) ExecuteWorkflow(
	ctx workflow.Context,
	in *interceptor.ExecuteWorkflowInput,
) (interface{}, error) {
	// Workflow starts here
	result, err := w.Next.ExecuteWorkflow(ctx, in)
	// Workflow ends here
	return result, err
}

type LoggedActivityInboundInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
	Next interceptor.ActivityInboundInterceptor
}

func NewLoggedActivityInboundInterceptor(next interceptor.ActivityInboundInterceptor) *LoggedActivityInboundInterceptor {
	return &LoggedActivityInboundInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		Next:                           next,
	}
}

func (c *LoggedActivityInboundInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	// Activity starts here
	out, err := c.Next.ExecuteActivity(ctx, in)
	// Activity ends here
	return out, err
}

type LoggedWorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (c LoggedWorkerInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return NewLoggedActivityInboundInterceptor(next)
}

func (c LoggedWorkerInterceptor) InterceptWorkflow(
	ctx workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	// Workflow intercepted here
	intercepted := NewLoggedWorkflowInboundInterceptor(next)
	// Workflow intercepting ends here
	return intercepted
}

func NewLoggedWorkerInterceptor() *LoggedWorkerInterceptor {
	return &LoggedWorkerInterceptor{
		WorkerInterceptorBase: interceptor.WorkerInterceptorBase{},
	}
}
