package model

import (
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type NameAndExclude struct {
	Exclude map[string]struct{}
	Name    string
}

func NewNameAndExclude(name string, exclude []string) NameAndExclude {
	var exset map[string]struct{}
	if len(exclude) != 0 {
		exset = make(map[string]struct{}, len(exclude))
		for _, col := range exclude {
			exset[col] = struct{}{}
		}
	}
	return NameAndExclude{Name: name, Exclude: exset}
}

type PullRecordsRequest struct {
	// record batch for pushing changes into
	RecordStream *CDCRecordStream
	// FlowJobName is the name of the flow job.
	FlowJobName string
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
	// LastOffset is the latest LSN that was synced.
	LastOffset int64
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
}

type Record interface {
	GetCheckpointID() int64
	GetCommitTime() time.Time
	GetDestinationTableName() string
	GetSourceTableName() string
	// get columns and values for the record
	GetItems() *RecordItems
	PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts)
}

type ToJSONOptions struct {
	UnnestColumns map[string]struct{}
	HStoreAsJSON  bool
}

func NewToJSONOptions(unnestCols []string, hstoreAsJSON bool) ToJSONOptions {
	var unnestColumns map[string]struct{}
	if len(unnestCols) != 0 {
		unnestColumns = make(map[string]struct{}, len(unnestCols))
		for _, col := range unnestCols {
			unnestColumns[col] = struct{}{}
		}
	}
	return ToJSONOptions{
		UnnestColumns: unnestColumns,
		HStoreAsJSON:  hstoreAsJSON,
	}
}

type BaseRecord struct {
	// CheckpointID is the ID of the record.
	CheckpointID int64 `json:"checkpointId"`
	// BeginMessage.CommitTime.UnixNano(), 16 bytes smaller than time.Time
	CommitTimeNano int64 `json:"commitTimeNano"`
}

func (r *BaseRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

func (r *BaseRecord) GetCommitTime() time.Time {
	return time.Unix(0, r.CommitTimeNano)
}

type InsertRecord struct {
	// Items is a map of column name to value.
	Items *RecordItems
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	BaseRecord
}

func (r *InsertRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *InsertRecord) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *InsertRecord) GetItems() *RecordItems {
	return r.Items
}

func (r *InsertRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.InsertCount++
	}
}

type UpdateRecord struct {
	// OldItems is a map of column name to value.
	OldItems *RecordItems
	// NewItems is a map of column name to value.
	NewItems *RecordItems
	// unchanged toast columns
	UnchangedToastColumns map[string]struct{}
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	BaseRecord
}

func (r *UpdateRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *UpdateRecord) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *UpdateRecord) GetItems() *RecordItems {
	return r.NewItems
}

func (r *UpdateRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.UpdateCount++
	}
}

type DeleteRecord struct {
	// Items is a map of column name to value.
	Items *RecordItems
	// unchanged toast columns, filled from latest UpdateRecord
	UnchangedToastColumns map[string]struct{}
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	BaseRecord
}

func (r *DeleteRecord) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *DeleteRecord) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *DeleteRecord) GetItems() *RecordItems {
	return r.Items
}

func (r *DeleteRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.DeleteCount++
	}
}

type TableWithPkey struct {
	TableName string
	// SHA256 hash of the primary key columns
	PkeyColVal [32]byte
}

type SyncRecordsRequest struct {
	Records *CDCRecordStream
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// Staging path for AVRO files in CDC
	StagingPath string
	// Lua script
	Script string
	// source:destination mappings
	TableMappings []*protos.TableMapping
	SyncBatchID   int64
}

type NormalizeRecordsRequest struct {
	TableNameSchemaMapping map[string]*protos.TableSchema
	FlowJobName            string
	SoftDeleteColName      string
	SyncedAtColName        string
	SyncBatchID            int64
	SoftDelete             bool
}

type SyncResponse struct {
	// TableNameRowsMapping tells how many records need to be synced to each destination table.
	TableNameRowsMapping map[string]*RecordTypeCounts
	// to be carried to parent workflow
	TableSchemaDeltas []*protos.TableSchemaDelta
	// LastSyncedCheckpointID is the last ID that was synced.
	LastSyncedCheckpointID int64
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced   int64
	CurrentSyncBatchID int64
}

type NormalizePayload struct {
	TableNameSchemaMapping map[string]*protos.TableSchema
	Done                   bool
	SyncBatchID            int64
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}

// being clever and passing the delta back as a regular record instead of heavy CDC refactoring.
type RelationRecord struct {
	TableSchemaDelta *protos.TableSchemaDelta `json:"tableSchemaDelta"`
	BaseRecord
}

func (r *RelationRecord) GetDestinationTableName() string {
	return r.TableSchemaDelta.DstTableName
}

func (r *RelationRecord) GetSourceTableName() string {
	return r.TableSchemaDelta.SrcTableName
}

func (r *RelationRecord) GetItems() *RecordItems {
	return nil
}

func (r *RelationRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
}

type RelationMessageMapping map[uint32]*pglogrepl.RelationMessage
