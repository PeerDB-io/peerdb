package shared

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	// Task Queues
	peerFlowTaskQueue     = "peer-flow-task-queue"
	snapshotFlowTaskQueue = "snapshot-flow-task-queue"

	// Signals
	FlowSignalName                 = "peer-flow-signal"
	CDCDynamicPropertiesSignalName = "cdc-dynamic-properties"
	NormalizeSyncSignalName        = "normalize-sync"
	NormalizeSyncDoneSignalName    = "normalize-sync-done"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-status"
	QRepFlowStateQuery = "q-qrep-flow-state"
	FlowStatusQuery    = "q-flow-status"

	// Updates
	FlowStatusUpdate = "u-flow-status"
)

const MirrorNameSearchAttribute = "MirrorName"

type (
	CDCFlowSignal int64
	ContextKey    string
)

const (
	NoopSignal CDCFlowSignal = iota
	ShutdownSignal
	PauseSignal

	FlowNameKey      ContextKey = "flowName"
	PartitionIDKey   ContextKey = "partitionId"
	DeploymentUIDKey ContextKey = "deploymentUid"
)

type TaskQueueID int64

const (
	PeerFlowTaskQueueID     TaskQueueID = iota
	SnapshotFlowTaskQueueID TaskQueueID = iota
)

const FetchAndChannelSize = 256 * 1024

func GetPeerFlowTaskQueueName(taskQueueID TaskQueueID) (string, error) {
	switch taskQueueID {
	case PeerFlowTaskQueueID:
		return prependUIDToTaskQueueName(peerFlowTaskQueue), nil
	case SnapshotFlowTaskQueueID:
		return prependUIDToTaskQueueName(snapshotFlowTaskQueue), nil
	default:
		return "", fmt.Errorf("unknown task queue id %d", taskQueueID)
	}
}

func prependUIDToTaskQueueName(taskQueueName string) string {
	deploymentUID := peerdbenv.PeerDBDeploymentUID()
	if deploymentUID == "" {
		return taskQueueName
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueName)
}
