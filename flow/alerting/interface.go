package alerting

import "context"

type AlertSender interface {
	sendAlert(ctx context.Context, alertTitle string, alertMessage string) error
	getSlotLagMBAlertThreshold() uint32
	getOpenConnectionsAlertThreshold() uint32
}
