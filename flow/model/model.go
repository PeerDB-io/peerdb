package model

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type PullRecordsRequest struct {
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// TableIdentifier is the identifier for the table.
	SourceTableIdentifier string
	// LastSyncedID is the last ID that was synced.
	LastSyncState *protos.LastSyncState
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
}

type Record interface {
	// GetCheckPointID returns the ID of the record.
	GetCheckPointID() int64
}

type InsertRecord struct {
	// CheckPointID is the ID of the record.
	CheckPointID int64
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
	// LastCheckPointID is the last ID that was pulled.
	// This is monotonically increasing, if a checkpoint with a greater ID is reached
	// on the destination, that means that the destination has already processed all
	// records up to this point.
	LastCheckPointID int64
}

type SyncRecordsRequest struct {
	Records *RecordBatch
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// TableIdentifier is the identifier for the raw table.
	DestinationTableIdentifier string
}

type NormalizeRecordsRequest struct {
	FlowJobName string
	// TableIdentifier is the identifier for the raw table.
	DestinationTableIdentifier string
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
