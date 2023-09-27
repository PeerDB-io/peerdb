package model

import (
	"encoding/json"
	"errors"
	"fmt"
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
	// schemas to allow adding new tables for, empty if MappingType != SCHEMA
	Schemas []string
	// should we add new tables detected in the source schemas?
	AllowTableAdditions bool
}

type Record interface {
	// GetCheckPointID returns the ID of the record.
	GetCheckPointID() int64
	// get table name
	GetTableName() string
	// get columns and values for the record
	GetItems() *RecordItems
}

type RecordItems struct {
	colToValIdx map[string]int
	values      []*qvalue.QValue
}

func NewRecordItems() *RecordItems {
	return &RecordItems{
		colToValIdx: make(map[string]int),
		// create a slice of 64 qvalues so that we don't have to allocate memory
		// for each record to reduce GC pressure
		values: make([]*qvalue.QValue, 0, 32),
	}
}

func NewRecordItemWithData(cols []string, val []*qvalue.QValue) *RecordItems {
	recordItem := NewRecordItems()
	for i, col := range cols {
		recordItem.colToValIdx[col] = len(recordItem.values)
		recordItem.values = append(recordItem.values, val[i])
	}
	return recordItem
}

func (r *RecordItems) AddColumn(col string, val *qvalue.QValue) {
	if idx, ok := r.colToValIdx[col]; ok {
		r.values[idx] = val
	} else {
		r.colToValIdx[col] = len(r.values)
		r.values = append(r.values, val)
	}
}

func (r *RecordItems) GetColumnValue(col string) *qvalue.QValue {
	if idx, ok := r.colToValIdx[col]; ok {
		return r.values[idx]
	}
	return nil
}

// UpdateIfNotExists takes in a RecordItems as input and updates the values of the
// current RecordItems with the values from the input RecordItems for the columns
// that are present in the input RecordItems but not in the current RecordItems.
// We return the slice of col names that were updated.
func (r *RecordItems) UpdateIfNotExists(input *RecordItems) []string {
	updatedCols := make([]string, 0)
	for col, idx := range input.colToValIdx {
		if _, ok := r.colToValIdx[col]; !ok {
			r.colToValIdx[col] = len(r.values)
			r.values = append(r.values, input.values[idx])
			updatedCols = append(updatedCols, col)
		}
	}
	return updatedCols
}

func (r *RecordItems) GetValueByColName(colName string) (*qvalue.QValue, error) {
	idx, ok := r.colToValIdx[colName]
	if !ok {
		return nil, fmt.Errorf("column name %s not found", colName)
	}
	return r.values[idx], nil
}

func (r *RecordItems) Len() int {
	return len(r.values)
}

func (r *RecordItems) ToJSON() (string, error) {
	if r.colToValIdx == nil {
		return "", errors.New("colToValIdx is nil")
	}
	jsonStruct := make(map[string]interface{})
	for col, idx := range r.colToValIdx {
		v := r.values[idx]
		var err error
		switch v.Kind {
		case qvalue.QValueKindString, qvalue.QValueKindJSON:
			if len(v.Value.(string)) > 15*1024*1024 {
				jsonStruct[col] = ""
			} else {
				jsonStruct[col] = v.Value
			}
		case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindDate,
			qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
			jsonStruct[col], err = v.GoTimeConvert()
			if err != nil {
				return "", err
			}
		case qvalue.QValueKindNumeric:
			bigRat, ok := v.Value.(*big.Rat)
			if !ok {
				return "", errors.New("expected *big.Rat value")
			}
			jsonStruct[col] = bigRat.FloatString(9)
		default:
			jsonStruct[col] = v.Value
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
	Items *RecordItems
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

func (r *InsertRecord) GetItems() *RecordItems {
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
	OldItems *RecordItems
	// NewItems is a map of column name to value.
	NewItems *RecordItems
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

func (r *UpdateRecord) GetItems() *RecordItems {
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
	Items *RecordItems
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

func (r *DeleteRecord) GetItems() *RecordItems {
	return r.Items
}

type TableWithPkey struct {
	TableName  string
	PkeyColVal qvalue.QValue
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
	// to be used to create additional tables, for MappingType SCHEMA
	AdditionalTableInfo *protos.AdditionalTableInfo
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}

// sync all the records normally, then apply any schema delta after NormalizeFlow.
// add any new tables at the end of SyncFlow.
type RecordsWithDeltaInfo struct {
	RecordBatch            *RecordBatch
	TableSchemaDelta       *protos.TableSchemaDelta
	RelationMessageMapping RelationMessageMapping
	AdditionalTableInfo    *protos.AdditionalTableInfo
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

func (r *RelationRecord) GetItems() *RecordItems {
	return nil
}

type RelationMessageMapping map[uint32]*protos.RelationMessage

// being clever and passing the new table back as a regular record instead of heavy refactoring in processMessage.
type AddedTableRecord struct {
	CheckPointID int64
	TableName    string
	SrcSchema    string
}

// Implement Record interface for RelationRecord.
func (r *AddedTableRecord) GetCheckPointID() int64 {
	return r.CheckPointID
}

func (r *AddedTableRecord) GetTableName() string {
	return r.TableName
}

func (r *AddedTableRecord) GetItems() *RecordItems {
	return nil
}
