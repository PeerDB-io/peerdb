package otel_metrics

const (
	PeerNameKey         = "peerName"
	SlotNameKey         = "slotName"
	FlowNameKey         = "flowName"
	DeploymentUidKey    = "deploymentUID"
	ErrorClassKey       = "errorClass"
	ErrorActionKey      = "errorAction"
	InstanceStatusKey   = "instanceStatus"
	WorkflowTypeKey     = "workflowType"
	BatchIdKey          = "batchId"
	SourcePeerType      = "sourcePeerType"
	DestinationPeerType = "destinationPeerType"
	SourcePeerName      = "sourcePeerName"
	DestinationPeerName = "destinationPeerName"
	FlowStatusKey       = "flowStatus"
	IsFlowResyncKey     = "isFlowResync"
)

const (
	InstanceStatusMaintenance = "maintenance"
	InstanceStatusUnknown     = "unknown"
	InstanceStatusReady       = "ready"
)
