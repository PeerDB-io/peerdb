package activities

import (
	"go.temporal.io/sdk/client"
)

type QRepLauncher struct {
	temporalClient *client.Client
}

func NewQRepLauncher(temporalClient *client.Client) *QRepLauncher {
}
