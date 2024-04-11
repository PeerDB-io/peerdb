package model

import (
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type Record interface {
	GetCheckpointID() int64
	GetCommitTime() time.Time
	GetDestinationTableName() string
	GetSourceTableName() string
	// get columns and values for the record
	GetItems() RecordItems
	PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts)
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
	Items RecordItems
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

func (r *InsertRecord) GetItems() RecordItems {
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
	OldItems RecordItems
	// NewItems is a map of column name to value.
	NewItems RecordItems
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

func (r *UpdateRecord) GetItems() RecordItems {
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
	Items RecordItems
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

func (r *DeleteRecord) GetItems() RecordItems {
	return r.Items
}

func (r *DeleteRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.DeleteCount++
	}
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

func (r *RelationRecord) GetItems() RecordItems {
	return RecordItems{ColToVal: nil}
}

func (r *RelationRecord) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
}
