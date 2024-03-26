package telemetry

import (
	"context"
)

type Sender interface {
	SendMessage(ctx context.Context, subject string, body string, attributes Attributes) (*string, error)
}

type Attributes struct {
	DeploymentUID string
	Type          string
	Level         Level
	Tags          []string
}

type Level string

const (
	INFO     Level = "INFO"
	WARN     Level = "WARN"
	ERROR    Level = "ERROR"
	CRITICAL Level = "CRITICAL"
)
