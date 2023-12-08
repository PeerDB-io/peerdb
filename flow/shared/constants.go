package shared

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
)

const (
	peerFlowTaskQueue      = "peer-flow-task-queue"
	snapshotFlowTaskQueue  = "snapshot-flow-task-queue"
	CDCFlowSignalName      = "peer-flow-signal"
	CDCBatchSizeSignalName = "cdc-batch-size-signal"
)

const MirrorNameSearchAttribute = "MirrorName"

type CDCFlowSignal int64
type ContextKey string

const (
	NoopSignal CDCFlowSignal = iota
	ShutdownSignal
	PauseSignal

	CDCMirrorMonitorKey ContextKey = "cdcMirrorMonitor"
	FlowNameKey         ContextKey = "flowName"
	PartitionIDKey      ContextKey = "partitionId"
	DeploymentUIDKey    ContextKey = "deploymentUid"
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
	deploymentUID := utils.GetEnvString("PEERDB_DEPLOYMENT_UID", "")
	if deploymentUID == "" {
		return taskQueueName
	}
	return fmt.Sprintf("%s-%s", deploymentUID, taskQueueName)
}
