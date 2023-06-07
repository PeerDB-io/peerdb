package shared

const (
	PeerFlowTaskQueue  = "peer-flow-task-queue"
	PeerFlowSignalName = "peer-flow-signal"
)

type PeerFlowSignal int64

const (
	NoopSignal PeerFlowSignal = iota
	ShutdownSignal
)
