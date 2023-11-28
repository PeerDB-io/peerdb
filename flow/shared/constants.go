package shared

import (
	"fmt"
	"os"
)

const (
	peerFlowTaskQueue     = "peer-flow-task-queue"
	snapshotFlowTaskQueue = "snapshot-flow-task-queue"
	CDCFlowSignalName     = "peer-flow-signal"
)

const MirrorNameSearchAttribute = "MirrorName"

type CDCFlowSignal int64
type ContextKey string

const (
	NoopSignal CDCFlowSignal = iota
	ShutdownSignal
	PauseSignal

	CDCMirrorMonitorKey ContextKey = "cdcMirrorMonitor"
)

type TaskQueueID int64

const (
	PeerFlowTaskQueueID     TaskQueueID = iota
	SnapshotFlowTaskQueueID TaskQueueID = iota
)

const FetchAndChannelSize = 256 * 1024

func GetPeerFlowTaskQueueName(taskQueueID TaskQueueID) (string, error) {
	deploymentUID := os.Getenv("PEERDB_DEPLOYMENT_UID")
	switch taskQueueID {
	case PeerFlowTaskQueueID:
		return deploymentUID + "-" + peerFlowTaskQueue, nil
	case SnapshotFlowTaskQueueID:
		return deploymentUID + "-" + snapshotFlowTaskQueue, nil
	default:
		return "", fmt.Errorf("unknown task queue id %d", taskQueueID)
	}
}
