package otel_metrics

const (
	PeerNameKey         string = "peerName"
	SlotNameKey         string = "slotName"
	FlowNameKey         string = "flowName"
	DeploymentUidKey    string = "deploymentUID"
	ErrorClassKey       string = "errorClass"
	InstanceStatusKey   string = "instanceStatus"
	WorkflowTypeKey     string = "workflowType"
	BatchIdKey          string = "batchId"
	SourcePeerType      string = "sourcePeerType"
	DestinationPeerType string = "destinationPeerType"
)

const (
	InstancestatusReady       string = "ready"
	InstancestatusMaintenance string = "maintenance"
	InstanceStatusUknown      string = "unknown"
)
