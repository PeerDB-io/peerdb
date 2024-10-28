package telemetry

import (
	"context"
)

type Sender interface {
	SendMessage(ctx context.Context, subject string, body string, attributes Attributes) (string, error)
}

type Attributes struct {
	DeploymentUID string
	Type          string
	Level         Level
	Tags          []string
}

type (
	Level           string
	IncidentIoLevel string
)

const (
	INFO     Level = "INFO"
	WARN     Level = "WARN"
	ERROR    Level = "ERROR"
	CRITICAL Level = "CRITICAL"

	// ClickHouse (incident.io) mapped alert levels
	IncMedium   IncidentIoLevel = "medium"
	IncWarning  IncidentIoLevel = "warning"
	IncHigh     IncidentIoLevel = "high"
	IncCritical IncidentIoLevel = "critical"
)

func ResolveIncidentIoLevels(level Level) IncidentIoLevel {
	switch level {
	case INFO:
		return IncMedium
	case WARN:
		return IncWarning
	case ERROR:
		return IncHigh
	case CRITICAL:
		return IncCritical
	default:
		return IncMedium
	}
}
