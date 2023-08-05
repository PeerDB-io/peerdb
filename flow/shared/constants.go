package shared

const (
	PeerFlowTaskQueue     = "peer-flow-task-queue"
	SnapshotFlowTaskQueue = "snapshot-flow-task-queue"
	PeerFlowSignalName    = "peer-flow-signal"
)

type PeerFlowSignal int64
type ContextKey string

const (
	NoopSignal PeerFlowSignal = iota
	ShutdownSignal
	EnableMetricsKey ContextKey = "enableMetrics"
)
