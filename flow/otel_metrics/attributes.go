package otel_metrics

const (
	PeerNameKey                = "peerName"
	SlotNameKey                = "slotName"
	FlowNameKey                = "flowName"
	DeploymentUidKey           = "deploymentUID"
	ErrorClassKey              = "errorClass"
	ErrorActionKey             = "errorAction"
	ErrorSourceKey             = "errorSource"
	ErrorCodeKey               = "errorCode"
	InstanceStatusKey          = "instanceStatus"
	WorkflowTypeKey            = "workflowType"
	BatchIdKey                 = "batchId"
	SourcePeerType             = "sourcePeerType"
	DestinationPeerType        = "destinationPeerType"
	SourcePeerName             = "sourcePeerName"
	DestinationPeerName        = "destinationPeerName"
	FlowStatusKey              = "flowStatus"
	IsFlowResyncKey            = "isFlowResync"
	FlowOperationKey           = "flowOperation"
	IsTemporalActivityKey      = "isTemporalActivity"
	IsTemporalLocalActivityKey = "isTemporalLocalActivity"
	TemporalActivityTypeKey    = "temporalActivityType"
	TemporalWorkflowTypeKey    = "temporalWorkflowType"
)

const (
	InstanceStatusMaintenance = "maintenance"
	InstanceStatusUnknown     = "unknown"
	InstanceStatusReady       = "ready"
)
