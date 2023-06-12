package model

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type PullRecordsRequest struct {
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// LastSyncedID is the last ID that was synced.
	LastSyncState *protos.LastSyncState
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
	//relId to name Mapping
	SrcTableIDNameMapping map[uint32]string
	// source to destination table name mapping
	TableNameMapping map[string]string
	// unchanged toast columns
	UnchangedToastColumns []string
	// tablename to schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
}

type Record interface {
	// GetCheckPointID returns the ID of the record.
	GetCheckPointID() int64
	// get table name
	GetTableName() string
	// get columns and values for the record
	GetItems() map[string]interface{}
}

type InsertRecord struct {
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	// Items is a map of column name to value.
	Items map[string]interface{}
	// unchanged toast columns
	UnchangedToastColumns []string
}

// Implement Record interface for InsertRecord.
func (r *InsertRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *InsertRecord) GetTableName() string {
	return r.DestinationTableName
}

func (r *InsertRecord) GetItems() map[string]interface{} {
	return r.Items
}

type UpdateRecord struct {
	// Name of the source table
	SourceTableName string
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// Name of the destination table
	DestinationTableName string
	// OldItems is a map of column name to value.
	OldItems map[string]interface{}
	// NewItems is a map of column name to value.
	NewItems map[string]interface{}
	// unchanged toast columns
	UnchangedToastColumns []string
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetTableName() string {
	return r.DestinationTableName
}

func (r *UpdateRecord) GetItems() map[string]interface{} {
	return r.NewItems
}

type DeleteRecord struct {
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// Items is a map of column name to value.
	Items map[string]interface{}
	// unchanged toast columns
	UnchangedToastColumns []string
}

// Implement Record interface for DeleteRecord.
func (r *DeleteRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *DeleteRecord) GetTableName() string {
	return r.SourceTableName
}

func (r *DeleteRecord) GetItems() map[string]interface{} {
	return r.Items
}

type TableWithPkey struct {
	TableName  string
	PkeyColVal interface{}
}

type RecordBatch struct {
	// Records are a list of json objects.
	Records []Record
	// FirstCheckPointID is the first ID that was pulled.
	FirstCheckPointID int64
	// LastCheckPointID is the last ID of the commit that corresponds to this batch.
	LastCheckPointID int64
	//TablePkey to record index mapping
	TablePKeyLastSeen map[TableWithPkey]int
}

type SyncRecordsRequest struct {
	Records *RecordBatch
	// FlowJobName is the name of the flow job.
	FlowJobName string
}

type NormalizeRecordsRequest struct {
	FlowJobName string
}

type SyncResponse struct {
	// FirstSyncedCheckPointID is the first ID that was synced.
	FirstSyncedCheckPointID int64
	// LastSyncedCheckPointID is the last ID that was synced.
	LastSyncedCheckPointID int64
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced int64
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}
