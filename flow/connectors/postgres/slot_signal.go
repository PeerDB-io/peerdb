package connpostgres

type SnapshotCreationResult struct {
	SlotName     string
	SnapshotName string
}

// This struct contains two signals.
// 1. SlotCreated - this can be waited on to ensure that the slot has been created.
// 2. CloneComplete - which can be waited on to ensure that the clone has completed.
type SnapshotSignal struct {
	SlotCreated   chan SnapshotCreationResult
	CloneComplete chan struct{}
	Error         chan error
}

// NewSnapshotSignal returns a new SlotSignal.
func NewSnapshotSignal() SnapshotSignal {
	return SnapshotSignal{
		SlotCreated:   make(chan SnapshotCreationResult, 1),
		CloneComplete: make(chan struct{}, 1),
		Error:         make(chan error, 1),
	}
}
