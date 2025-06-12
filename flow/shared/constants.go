package shared

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

var Year0000 = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)

type (
	ContextKey  string
	TaskQueueID string
)

const (
	// Task Queues
	PeerFlowTaskQueue        TaskQueueID = "peer-flow-task-queue"
	SnapshotFlowTaskQueue    TaskQueueID = "snapshot-flow-task-queue"
	MaintenanceFlowTaskQueue TaskQueueID = "maintenance-flow-task-queue"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-state"
	QRepFlowStateQuery = "q-qrep-flow-state"
	FlowStatusQuery    = "q-flow-status"
)

var MirrorNameSearchAttribute = temporal.NewSearchAttributeKeyString("MirrorName")

func NewSearchAttributes(mirrorName string) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(MirrorNameSearchAttribute.ValueSet(mirrorName))
}

const (
	FlowNameKey      ContextKey = "flowName"
	PartitionIDKey   ContextKey = "partitionId"
	DeploymentUIDKey ContextKey = "deploymentUid"
)

const FetchAndChannelSize = 256 * 1024

func Ptr[T any](x T) *T {
	return &x
}
