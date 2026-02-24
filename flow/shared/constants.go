package shared

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

var Year0000 = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	MoneyOID        uint32 = 790
	TxidSnapshotOID uint32 = 2970
	TsvectorOID     uint32 = 3614
	TsqueryOID      uint32 = 3615
)

// PeerDB Internal Version System
//
// This versioning system allows PeerDB to introduce breaking changes to data formats,
// connector behavior, and destination-specific settings in a backward-compatible way.
//
// ## How it works:
//   - Each mirror is created with an internal version number stored in the
//     FlowConnectionConfigsCore.Version field in the catalog database
//   - The version is set once when a mirror is created and persists for the lifetime of that mirror
//   - New mirrors are created with InternalVersion_Latest
//   - Existing mirrors continue using their original version, ensuring stable behavior across upgrades
//
// ## How to add a new version:
//  1. Add a new constant below with a descriptive name and a comment explaining the change
//  2. Add version checks in the relevant connector code (e.g., if version >= InternalVersion_MyFeature)
//  3. Add e2e test to verify both old and new versions work correctly (old mirrors keep old behavior, new mirrors get new behavior)

const (
	InternalVersion_First uint32 = iota
	// Postgres: vector types ("vector", "halfvec", "sparsevec") replicated as float arrays instead of string
	InternalVersion_PgVectorAsFloatArray
	// MongoDB: rename `_full_document` column to `doc`
	InternalVersion_MongoDBFullDocumentColumnToDoc
	// All: setting json_type_escape_dots_in_keys = true when inserting JSON column to ClickHouse (only impacts MongoDB today)
	InternalVersion_JsonEscapeDotsInKeys
	// MongoDB: `_id` column values stored as-is without redundant quotes
	InternalVersion_MongoDBIdWithoutRedundantQuotes

	TotalNumberOfInternalVersions
	InternalVersion_Latest = TotalNumberOfInternalVersions - 1
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

func Ptr[T any](x T) *T {
	return &x
}

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
