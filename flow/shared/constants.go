package shared

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

const (
	peerFlowTaskQueue      = "peer-flow-task-queue"
	CDCFlowSignalName      = "peer-flow-signal"
	CDCBatchSizeSignalName = "cdc-batch-size-signal"
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

const FetchAndChannelSize = 256 * 1024

func GetPeerFlowTaskQueueName() string {
	return prependUIDToTaskQueueName(peerFlowTaskQueue)
}

func prependUIDToTaskQueueName(taskQueueName string) string {
	deploymentUID := peerdbenv.PeerDBDeploymentUID()
	if deploymentUID == "" {
		return taskQueueName
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueName)
}
