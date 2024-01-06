package activities

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
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

	deploymentUIDPrefix := ""
	if peerdbenv.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", peerdbenv.PeerDBDeploymentUID())
	}

	slotLagInMBThreshold := peerdbenv.PeerDBSlotLagMBAlertThreshold()
	if (slotLagInMBThreshold > 0) && (slotInfo[0].LagInMb >= float32(slotLagInMBThreshold)) {
		a.Alerter.AlertIf(ctx, fmt.Sprintf("%s-slot-lag-threshold-exceeded", peerName),
			fmt.Sprintf(`%sSlot `+"`%s`"+` on peer `+"`%s`"+` has exceeded threshold size of %dMB, currently at %.2fMB!
cc: <!channel>`,
				deploymentUIDPrefix, slotName, peerName, slotLagInMBThreshold, slotInfo[0].LagInMb))
	}

	// Also handles alerts for PeerDB user connections exceeding a given limit here
	maxOpenConnectionsThreshold := peerdbenv.PeerDBOpenConnectionsAlertThreshold()
	res, err := srcConn.GetOpenConnectionsForUser()
	if err != nil {
		slog.WarnContext(ctx, "warning: failed to get current open connections", slog.Any("error", err))
		return err
	}
	if (maxOpenConnectionsThreshold > 0) && (res.CurrentOpenConnections >= int64(maxOpenConnectionsThreshold)) {
		a.Alerter.AlertIf(ctx, fmt.Sprintf("%s-max-open-connections-threshold-exceeded", peerName),
			fmt.Sprintf(`%sOpen connections from PeerDB user `+"`%s`"+` on peer `+"`%s`"+
				` has exceeded threshold size of %d connections, currently at %d connections!
cc: <!channel>`,
				deploymentUIDPrefix, res.UserName, peerName, maxOpenConnectionsThreshold, res.CurrentOpenConnections))
	}

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
