package connpostgres

type SlotCreationResult struct {
	SlotName     string
	SnapshotName string
	Err          error
}

// This struct contains two signals.
// 1. SlotCreated - this can be waited on to ensure that the slot has been created.
// 2. CloneComplete - which can be waited on to ensure that the clone has completed.
type SlotSignal struct {
	SlotCreated   chan *SlotCreationResult
	CloneComplete chan bool
}

// NewSlotSignal returns a new SlotSignal.
func NewSlotSignal() *SlotSignal {
	return &SlotSignal{
		SlotCreated:   make(chan *SlotCreationResult, 1),
		CloneComplete: make(chan bool, 1),
	}
}
