package telemetry

import (
	"context"
)

type Sender interface {
	SendMessage(ctx context.Context, subject string, body string, attributes Attributes) (*string, error)
}

type Attributes struct {
	Level         Level
	DeploymentUID string
	Tags          []string
	Type          string
}

type Level string

const (
	INFO     Level = "INFO"
	WARN           = "WARN"
	ERROR          = "ERROR"
	CRITICAL       = "CRITICAL"
)
