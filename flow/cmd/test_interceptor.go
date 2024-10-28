package cmd

import (
	"context"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
	"log"
)

type CommonWorkflowInboundInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
	Next interceptor.WorkflowInboundInterceptor
}

func NewCommonWorkflowInboundInterceptor(next interceptor.WorkflowInboundInterceptor) *CommonWorkflowInboundInterceptor {
	return &CommonWorkflowInboundInterceptor{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		Next:                           next,
	}
}

func (w *CommonWorkflowInboundInterceptor) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (interface{}, error) {
	log.Print("[kg] workflow is starting")
	result, err := w.Next.ExecuteWorkflow(ctx, in)
	// Ignore cancelled, continue as new
	if err != nil {
		log.Printf("[kg] workflow is failing with error: %+v", err)
	}
	return result, err
}

type CommonActivityInboundInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
	Next interceptor.ActivityInboundInterceptor
}

func NewCommonActivityInboundInterceptor(next interceptor.ActivityInboundInterceptor) *CommonActivityInboundInterceptor {
	return &CommonActivityInboundInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		Next:                           next,
	}
}

func (c *CommonActivityInboundInterceptor) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (interface{}, error) {
	log.Printf("[kg] activity is starting")
	out, err := c.Next.ExecuteActivity(ctx, in)
	if err != nil {
		log.Printf("[kg] activity is failing with error: %+v", err)
	}
	return out, err
}

type CommonWorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (c CommonWorkerInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return NewCommonActivityInboundInterceptor(next)
}

func (c CommonWorkerInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	log.Printf("[kg] intercepting workflow")
	intercepted := NewCommonWorkflowInboundInterceptor(next)
	log.Printf("[kg] intercepted workflow")
	return intercepted
}

//func (c CommonWorkerInterceptor) mustEmbedWorkerInterceptorBase() {
//}

func NewCommonWorkerInterceptor() *CommonWorkerInterceptor {
	return &CommonWorkerInterceptor{
		WorkerInterceptorBase: interceptor.WorkerInterceptorBase{},
	}
}
