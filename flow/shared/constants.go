package shared

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

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

func GetPeerFlowTaskQueueName(taskQueueID TaskQueueID) string {
	deploymentUID := peerdbenv.PeerDBDeploymentUID()
	if deploymentUID == "" {
		return string(taskQueueID)
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueID)
}

func GetDeploymentUID() string {
	return peerdbenv.PeerDBDeploymentUID()
}
