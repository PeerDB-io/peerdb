package peerflow

import (
	"go.temporal.io/sdk/worker"
)

func RegisterFlowWorkerWorkflows(w worker.WorkflowRegistry) {
	w.RegisterWorkflow(CDCFlowWorkflowWithConfig)
	w.RegisterWorkflow(DropFlowWorkflow)
	w.RegisterWorkflow(NormalizeFlowWorkflow)
	w.RegisterWorkflow(SetupFlowWorkflow)
	w.RegisterWorkflow(SyncFlowWorkflow)
	w.RegisterWorkflow(QRepFlowWorkflow)
	w.RegisterWorkflow(QRepPartitionWorkflow)
	w.RegisterWorkflow(XminFlowWorkflow)

	w.RegisterWorkflow(GlobalScheduleManagerWorkflow)
	w.RegisterWorkflow(HeartbeatFlowWorkflow)
	w.RegisterWorkflow(RecordSlotSizeWorkflow)
}
