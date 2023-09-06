package model

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
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
	// tablename to schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
	// override publication name
	OverridePublicationName string
	// override replication slot name
	OverrideReplicationSlotName string
	// for supporting schema changes
	RelationMessageMapping RelationMessageMapping
}

type Record interface {
	// GetCheckPointID returns the ID of the record.
	GetCheckPointID() int64
	// get table name
	GetTableName() string
	// get columns and values for the record
	GetItems() RecordItems
}

type RecordItems map[string]qvalue.QValue

func (r RecordItems) ToJSON() (string, error) {
	jsonStruct := make(map[string]interface{})
	for k, v := range r {
		var err error
		switch v.Kind {
		case qvalue.QValueKindString, qvalue.QValueKindJSON:
			if len(v.Value.(string)) > 15*1024*1024 {
				jsonStruct[k] = ""
			} else {
				jsonStruct[k] = v.Value
			}
		case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindDate,
			qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
			jsonStruct[k], err = v.GoTimeConvert()
			if err != nil {
				return "", err
			}
		case qvalue.QValueKindNumeric:
			bigRat := v.Value.(*big.Rat)
			jsonStruct[k] = bigRat.FloatString(9)
		default:
			jsonStruct[k] = v.Value
		}
	}
	jsonBytes, err := json.Marshal(jsonStruct)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
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
	Items RecordItems
	// unchanged toast columns
	UnchangedToastColumns map[string]bool
}

// Implement Record interface for InsertRecord.
func (r *InsertRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *InsertRecord) GetTableName() string {
	return r.DestinationTableName
}

func (r *InsertRecord) GetItems() RecordItems {
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
	OldItems RecordItems
	// NewItems is a map of column name to value.
	NewItems RecordItems
	// unchanged toast columns
	UnchangedToastColumns map[string]bool
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

// Implement Record interface for UpdateRecord.
func (r *UpdateRecord) GetTableName() string {
	return r.DestinationTableName
}

func (r *UpdateRecord) GetItems() RecordItems {
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
	Items RecordItems
	// unchanged toast columns
	UnchangedToastColumns map[string]bool
}

// Implement Record interface for DeleteRecord.
func (r *DeleteRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *DeleteRecord) GetTableName() string {
	return r.SourceTableName
}

func (r *DeleteRecord) GetItems() RecordItems {
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
	// SyncMode to use for pushing raw records
	SyncMode protos.QRepSyncMode
	// Staging path for AVRO files in CDC
	StagingPath string
	// PushBatchSize is the number of records to push in a batch for EventHub.
	PushBatchSize int64
	// PushParallelism is the number of batches in Event Hub to push in parallel.
	PushParallelism int64
}

type NormalizeRecordsRequest struct {
	FlowJobName string
	SoftDelete  bool
}

type SyncResponse struct {
	// FirstSyncedCheckPointID is the first ID that was synced.
	FirstSyncedCheckPointID int64
	// LastSyncedCheckPointID is the last ID that was synced.
	LastSyncedCheckPointID int64
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced int64
	// CurrentSyncBatchID is the ID of the currently synced batch.
	CurrentSyncBatchID int64
	// TableNameRowsMapping tells how many records need to be synced to each destination table.
	TableNameRowsMapping map[string]uint32
	// to be carried to NormalizeFlow
	TableSchemaDelta *protos.TableSchemaDelta
	// to be stored in state for future PullFlows
	RelationMessageMapping RelationMessageMapping
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}

// sync all the records normally, then apply the schema delta after NormalizeFlow.
type RecordsWithTableSchemaDelta struct {
	RecordBatch            *RecordBatch
	TableSchemaDelta       *protos.TableSchemaDelta
	RelationMessageMapping RelationMessageMapping
}

// being clever and passing the delta back as a regular record instead of heavy CDC refactoring.
type RelationRecord struct {
	CheckPointID     int64
	TableSchemaDelta *protos.TableSchemaDelta
}

// Implement Record interface for RelationRecord.
func (r *RelationRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *RelationRecord) GetTableName() string {
	return r.TableSchemaDelta.SrcTableName
}

func (r *RelationRecord) GetItems() RecordItems {
	return nil
}

type RelationMessageMapping map[uint32]*protos.RelationMessage
