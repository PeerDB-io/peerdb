package model

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type NameAndExclude struct {
	Name    string
	Exclude map[string]struct{}
}

func NewNameAndExclude(name string, exclude []string) NameAndExclude {
	exset := make(map[string]struct{}, len(exclude))
	for _, col := range exclude {
		exset[col] = struct{}{}
	}
	return NameAndExclude{Name: name, Exclude: exset}
}

type PullRecordsRequest struct {
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// LastOffset is the latest LSN that was synced.
	LastOffset int64
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
	// relId to name Mapping
	SrcTableIDNameMapping map[uint32]string
	// source to destination table name mapping
	TableNameMapping map[string]NameAndExclude
	// tablename to schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
	// override publication name
	OverridePublicationName string
	// override replication slot name
	OverrideReplicationSlotName string
	// for supporting schema changes
	RelationMessageMapping RelationMessageMapping
	// record batch for pushing changes into
	RecordStream *CDCRecordStream
}

type Record interface {
	// GetCheckpointID returns the ID of the record.
	GetCheckpointID() int64
	// get table name
	GetDestinationTableName() string
	// get columns and values for the record
	GetItems() *RecordItems
}

type ToJSONOptions struct {
	UnnestColumns map[string]struct{}
	HStoreAsJSON  bool
}

func NewToJSONOptions(unnestCols []string, hstoreAsJSON bool) *ToJSONOptions {
	unnestColumns := make(map[string]struct{}, len(unnestCols))
	for _, col := range unnestCols {
		unnestColumns[col] = struct{}{}
	}
	return &ToJSONOptions{
		UnnestColumns: unnestColumns,
		HStoreAsJSON:  hstoreAsJSON,
	}
}

type InsertRecord struct {
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CheckpointID is the ID of the record.
	CheckpointID int64
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	// Items is a map of column name to value.
	Items *RecordItems
}

// Implement Record interface for InsertRecord.
func (r *InsertRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

func (r *InsertRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *InsertRecord) GetItems() *RecordItems {
	return r.Items
}

type UpdateRecord struct {
	// Name of the source table
	SourceTableName string
	// CheckpointID is the ID of the record.
	CheckpointID int64
	// Name of the destination table
	DestinationTableName string
	// OldItems is a map of column name to value.
	OldItems *RecordItems
	// NewItems is a map of column name to value.
	NewItems *RecordItems
	// unchanged toast columns
	UnchangedToastColumns map[string]struct{}
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *UpdateRecord) GetItems() *RecordItems {
	return r.NewItems
}

type DeleteRecord struct {
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CheckpointID is the ID of the record.
	CheckpointID int64
	// Items is a map of column name to value.
	Items *RecordItems
	// unchanged toast columns, filled from latest UpdateRecord
	UnchangedToastColumns map[string]struct{}
}

// Implement Record interface for DeleteRecord.
func (r *DeleteRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

func (r *DeleteRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *DeleteRecord) GetItems() *RecordItems {
	return r.Items
}

type TableWithPkey struct {
	TableName string
	// SHA256 hash of the primary key columns
	PkeyColVal [32]byte
}

type SyncRecordsRequest struct {
	SyncBatchID int64
	Records     *CDCRecordStream
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// source:destination mappings
	TableMappings []*protos.TableMapping
	// Staging path for AVRO files in CDC
	StagingPath string
}

type NormalizeRecordsRequest struct {
	FlowJobName            string
	SyncBatchID            int64
	SoftDelete             bool
	SoftDeleteColName      string
	SyncedAtColName        string
	TableNameSchemaMapping map[string]*protos.TableSchema
}

type SyncResponse struct {
	// LastSyncedCheckpointID is the last ID that was synced.
	LastSyncedCheckpointID int64
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced int64
	// CurrentSyncBatchID is the ID of the currently synced batch.
	CurrentSyncBatchID int64
	// TableNameRowsMapping tells how many records need to be synced to each destination table.
	TableNameRowsMapping map[string]uint32
	// to be carried to parent WorkFlow
	TableSchemaDeltas []*protos.TableSchemaDelta
	// to be stored in state for future PullFlows
	RelationMessageMapping RelationMessageMapping
}

type NormalizePayload struct {
	Done                   bool
	SyncBatchID            int64
	TableNameSchemaMapping map[string]*protos.TableSchema
}

type NormalizeFlowResponse struct {
	Results []NormalizeResponse
	Errors  []string
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}

// being clever and passing the delta back as a regular record instead of heavy CDC refactoring.
type RelationRecord struct {
	CheckpointID     int64                    `json:"checkpointId"`
	TableSchemaDelta *protos.TableSchemaDelta `json:"tableSchemaDelta"`
}

// Implement Record interface for RelationRecord.
func (r *RelationRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

func (r *RelationRecord) GetDestinationTableName() string {
	return r.TableSchemaDelta.DstTableName
}

func (r *RelationRecord) GetItems() *RecordItems {
	return nil
}

type RelationMessageMapping map[uint32]*protos.RelationMessage
