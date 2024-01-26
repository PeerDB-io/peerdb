package activities

import (
	"context"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
)

func (a *FlowableActivity) recordSlotSizePeriodically(
	ctx context.Context,
	srcConn connectors.CDCPullConnector,
	slotName string,
	peerName string,
) {
	// ensures slot info is logged at least once per SyncFlow
	err := srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, slotName, peerName)
	if err != nil {
		return
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := srcConn.HandleSlotInfo(ctx, a.Alerter, a.CatalogPool, slotName, peerName)
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
