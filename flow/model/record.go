package model

import (
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type Record[T Items] interface {
	Kind() string
	GetCheckpointID() int64
	GetCommitTime() time.Time
	GetDestinationTableName() string
	GetSourceTableName() string
	// get columns and values for the record
	GetItems() T
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

type InsertRecord[T Items] struct {
	// Items is a map of column name to value.
	Items T
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	BaseRecord
}

func (*InsertRecord[T]) Kind() string {
	return "insert"
}

func (r *InsertRecord[T]) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *InsertRecord[T]) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *InsertRecord[T]) GetItems() T {
	return r.Items
}

func (r *InsertRecord[T]) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.InsertCount.Add(1)
	}
}

type UpdateRecord[T Items] struct {
	// OldItems is a map of column name to value.
	OldItems T
	// NewItems is a map of column name to value.
	NewItems T
	// unchanged toast columns
	UnchangedToastColumns map[string]struct{}
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	BaseRecord
}

func (*UpdateRecord[T]) Kind() string {
	return "update"
}

func (r *UpdateRecord[T]) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *UpdateRecord[T]) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *UpdateRecord[T]) GetItems() T {
	return r.NewItems
}

func (r *UpdateRecord[T]) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.UpdateCount.Add(1)
	}
}

type DeleteRecord[T Items] struct {
	// Items is a map of column name to value.
	Items T
	// unchanged toast columns, filled from latest UpdateRecord
	UnchangedToastColumns map[string]struct{}
	// Name of the source table
	SourceTableName string
	// Name of the destination table
	DestinationTableName string
	BaseRecord
}

func (*DeleteRecord[T]) Kind() string {
	return "delete"
}

func (r *DeleteRecord[T]) GetDestinationTableName() string {
	return r.DestinationTableName
}

func (r *DeleteRecord[T]) GetSourceTableName() string {
	return r.SourceTableName
}

func (r *DeleteRecord[T]) GetItems() T {
	return r.Items
}

func (r *DeleteRecord[T]) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTableName]
	if ok {
		recordCount.DeleteCount.Add(1)
	}
}

// being clever and passing the delta back as a regular record instead of heavy CDC refactoring.
type RelationRecord[T Items] struct {
	TableSchemaDelta *protos.TableSchemaDelta `json:"tableSchemaDelta"`
	BaseRecord
}

func (*RelationRecord[T]) Kind() string {
	return "relation"
}

func (r *RelationRecord[T]) GetDestinationTableName() string {
	return r.TableSchemaDelta.DstTableName
}

func (r *RelationRecord[T]) GetSourceTableName() string {
	return r.TableSchemaDelta.SrcTableName
}

func (r *RelationRecord[T]) GetItems() T {
	var none T
	return none
}

func (r *RelationRecord[T]) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
}

type MessageRecord[T Items] struct {
	Prefix  string
	Content string
	BaseRecord
}

func (*MessageRecord[T]) Kind() string {
	return "message"
}

func (r *MessageRecord[T]) GetDestinationTableName() string {
	return ""
}

func (r *MessageRecord[T]) GetSourceTableName() string {
	return ""
}

func (r *MessageRecord[T]) GetItems() T {
	var none T
	return none
}

func (r *MessageRecord[T]) PopulateCountMap(mapOfCounts map[string]*RecordTypeCounts) {
}
