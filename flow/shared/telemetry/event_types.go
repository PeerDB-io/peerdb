package telemetry

type EventType string

const (
	CreatePeer       EventType = "CreatePeer"
	CreateMirror     EventType = "CreateMirror"
	StartMaintenance EventType = "StartMaintenance"
	EndMaintenance   EventType = "EndMaintenance"
	MaintenanceWait  EventType = "MaintenanceWait"

	Other EventType = "Other"
)
