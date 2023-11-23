package shared

const (
	PeerFlowTaskQueue     = "peer-flow-task-queue"
	SnapshotFlowTaskQueue = "snapshot-flow-task-queue"
	CDCFlowSignalName     = "peer-flow-signal"
)

type CDCFlowSignal int64
type ContextKey string

const (
	NoopSignal CDCFlowSignal = iota
	ShutdownSignal
	PauseSignal

	CDCMirrorMonitorKey ContextKey = "cdcMirrorMonitor"
)

const FetchAndChannelSize = 256 * 1024
