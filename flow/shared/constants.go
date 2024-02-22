package shared

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	// Task Queues
	peerFlowTaskQueue     = "peer-flow-task-queue"
	snapshotFlowTaskQueue = "snapshot-flow-task-queue"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-state"
	QRepFlowStateQuery = "q-qrep-flow-state"
	FlowStatusQuery    = "q-flow-status"

	// Updates
	FlowStatusUpdate = "u-flow-status"
)

const MirrorNameSearchAttribute = "MirrorName"

type (
	ContextKey string
)

const (
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

func GetDeploymentUID() string {
	return peerdbenv.PeerDBDeploymentUID()
}
