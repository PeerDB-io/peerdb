package shared

import "errors"

type (
	ContextKey  string
	TaskQueueID string
)

const (
	// Task Queues
	PeerFlowTaskQueue     TaskQueueID = "peer-flow-task-queue"
	SnapshotFlowTaskQueue TaskQueueID = "snapshot-flow-task-queue"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-state"
	QRepFlowStateQuery = "q-qrep-flow-state"
	FlowStatusQuery    = "q-flow-status"

	// Updates
	FlowStatusUpdate = "u-flow-status"
)

const MirrorNameSearchAttribute = "MirrorName"

const (
	FlowNameKey      ContextKey = "flowName"
	PartitionIDKey   ContextKey = "partitionId"
	DeploymentUIDKey ContextKey = "deploymentUid"
)

const FetchAndChannelSize = 256 * 1024

var ErrUnusualNormalize = errors.New("unusual normalize detected")
