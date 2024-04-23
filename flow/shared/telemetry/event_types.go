package telemetry

type EventType string

const (
	CreatePeer   EventType = "CreatePeer"
	CreateMirror EventType = "CreateMirror"
	Other        EventType = "Other"
)
