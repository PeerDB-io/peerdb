package activities

import (
	"context"
	"fmt"
	"log/slog"
)

type PeerType string

const (
	Source      PeerType = "source"
	Destination PeerType = "destination"
)

func (a *FlowableActivity) getPeerNameForMirror(ctx context.Context, flowName string, peerType PeerType) (string, error) {
	peerClause := "source_peer"
	if peerType == Destination {
		peerClause = "destination_peer"
	}
	q := fmt.Sprintf("SELECT p.name FROM flows f JOIN peers p ON f.%s = p.id WHERE f.name = $1;", peerClause)
	var peerName string
	err := a.CatalogPool.QueryRow(ctx, q, flowName).Scan(&peerName)
	if err != nil {
		slog.Error("failed to get peer name for flow", slog.String("flow_name", flowName), slog.Any("error", err))
		return "", fmt.Errorf("failed to get peer name for flow %s: %w", flowName, err)
	}

	return peerName, nil
}
