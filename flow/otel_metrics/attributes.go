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
	PeerDBVersionKey           = "peerDBVersion"
	DeploymentVersionKey       = "deploymentVersion"
	WorkflowTypeKey            = "workflowType"
	BatchIdKey                 = "batchId"
	SourcePeerType             = "sourcePeerType"
	SourcePeerVariant          = "sourcePeerVariant"
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
	IsFlowActiveKey            = "isFlowActive"
	DestinationTableNameKey    = "destinationTableName"
)

const (
	RecordOperationTypeKey = "recordOperationType"
)

const (
	RecordOperationTypeInsert = "insert"
	RecordOperationTypeUpdate = "update"
	RecordOperationTypeDelete = "delete"
)

const (
	InstanceStatusMaintenance = "maintenance"
	InstanceStatusUnknown     = "unknown"
	InstanceStatusReady       = "ready"
)
