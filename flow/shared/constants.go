package shared

import (
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

var Year0000 = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)

// The PeerDB Internal Version System lives in flow/pkg/common (see the
// documentation there); the aliases below keep existing importers of this
// package working.
const (
	InternalVersion_First                           = common.InternalVersion_First
	InternalVersion_PgVectorAsFloatArray            = common.InternalVersion_PgVectorAsFloatArray
	InternalVersion_MongoDBFullDocumentColumnToDoc  = common.InternalVersion_MongoDBFullDocumentColumnToDoc
	InternalVersion_JsonEscapeDotsInKeys            = common.InternalVersion_JsonEscapeDotsInKeys
	InternalVersion_MongoDBIdWithoutRedundantQuotes = common.InternalVersion_MongoDBIdWithoutRedundantQuotes
	InternalVersion_MySQL5ConvertEnumsToInts        = common.InternalVersion_MySQL5ConvertEnumsToInts
	InternalVersion_MySQLConvertBitToUInt64         = common.InternalVersion_MySQLConvertBitToUInt64
	InternalVersion_MySQL5ConvertSetsToInts         = common.InternalVersion_MySQL5ConvertSetsToInts

	TotalNumberOfInternalVersions = common.TotalNumberOfInternalVersions
	InternalVersion_Latest        = common.InternalVersion_Latest
)

type (
	ContextKey  string
	TaskQueueID string
)

func (c ContextKey) String() string {
	return string(c)
}

const (
	// Task Queues
	PeerFlowTaskQueue        TaskQueueID = "peer-flow-task-queue"
	SnapshotFlowTaskQueue    TaskQueueID = "snapshot-flow-task-queue"
	MaintenanceFlowTaskQueue TaskQueueID = "maintenance-flow-task-queue"

	// Queries
	CDCFlowStateQuery  = "q-cdc-flow-state"
	QRepFlowStateQuery = "q-qrep-flow-state"
)

var MirrorNameSearchAttribute = temporal.NewSearchAttributeKeyString("MirrorName")

func NewSearchAttributes(mirrorName string) temporal.SearchAttributes {
	return temporal.NewSearchAttributes(MirrorNameSearchAttribute.ValueSet(mirrorName))
}

const (
	FlowNameKey      ContextKey = "flowName"
	PartitionIDKey   ContextKey = "partitionId"
	DeploymentUIDKey ContextKey = "deploymentUid"
	RequestIdKey     ContextKey = "x-peerdb-request-id"
)

const (
	QRepFetchSize   = 128 * 1024
	QRepChannelSize = 1024
)

func Val[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// Flag constants for flow config Flags mapping
const (
	Flag_ClickHouseTime64Enabled = "clickhouse_time64_enabled"
)
