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
}

type Record interface {
	// GetCheckPointID returns the ID of the record.
	GetCheckPointID() int64
}

type InsertRecord struct {
	// Name of the destination table
	DestinationTableName string
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	// Items is a map of column name to value.
	Items map[string]interface{}
}

// Implement Record interface for InsertRecord.
func (r *InsertRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

type UpdateRecord struct {
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// Name of the destination table
	DestinationTableName string
	// OldItems is a map of column name to value.
	OldItems map[string]interface{}
	// NewItems is a map of column name to value.
	NewItems map[string]interface{}
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

type DeleteRecord struct {
	// Name of the destination table
	DestinationTableName string
	// CheckPointID is the ID of the record.
	CheckPointID int64
	// Items is a map of column name to value.
	Items map[string]interface{}
}

// Implement Record interface for DeleteRecord.
func (r *DeleteRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

type RecordBatch struct {
	// Records are a list of json objects.
	Records []Record
	// FirstCheckPointID is the first ID that was pulled.
	FirstCheckPointID int64
	// LastCheckPointID is the last ID of the commit that corresponds to this batch.
	LastCheckPointID int64
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
