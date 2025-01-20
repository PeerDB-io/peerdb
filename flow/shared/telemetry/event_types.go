package telemetry

type EventType string

const (
	CreatePeer       EventType = "CreatePeer"
	CreateMirror     EventType = "CreateMirror"
	EditMirror       EventType = "EditMirror"
	StartMaintenance EventType = "StartMaintenance"
	EndMaintenance   EventType = "EndMaintenance"
	MaintenanceWait  EventType = "MaintenanceWait"

	Other EventType = "Other"
)
