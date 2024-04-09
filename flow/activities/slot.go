package activities

import (
	"context"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
)

func (a *FlowableActivity) handleSlotInfo(
	ctx context.Context,
	srcConn connectors.CDCPullConnector,
	slotName string,
	peerName string,
) error {
	slotInfo, err := srcConn.GetSlotInfo(slotName)
	if err != nil {
		slog.WarnContext(ctx, "warning: failed to get slot info", slog.Any("error", err))
		return err
	}

	if len(slotInfo) == 0 {
		slog.WarnContext(ctx, "warning: unable to get slot info", slog.Any("slotName", slotName))
		return nil
	}

	a.Alerter.AlertIfSlotLag(ctx, peerName, slotInfo[0])

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	res, err := srcConn.GetOpenConnectionsForUser()
	if err != nil {
		slog.WarnContext(ctx, "warning: failed to get current open connections", slog.Any("error", err))
		return err
	}

	a.Alerter.AlertIfOpenConnections(ctx, peerName, res)

	if len(slotInfo) != 0 {
		return monitoring.AppendSlotSizeInfo(ctx, a.CatalogPool, peerName, slotInfo[0])
	}
	return nil
}

func (a *FlowableActivity) recordSlotSizePeriodically(
	ctx context.Context,
	srcConn connectors.CDCPullConnector,
	slotName string,
	peerName string,
) {
	// ensures slot info is logged at least once per SyncFlow
	err := a.handleSlotInfo(ctx, srcConn, slotName, peerName)
	if err != nil {
		return
	}

	timeout := 5 * time.Minute
	ticker := time.NewTicker(timeout)

	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := a.handleSlotInfo(ctx, srcConn, slotName, peerName)
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
		ticker.Reset(timeout)
	}
}
