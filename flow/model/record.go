package model

import (
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type Record[T Items] interface {
	Kind() string
	GetCheckpointID() int64
	GetCommitTime() time.Time
	GetTransactionID() uint64
	GetDestinationTable() common.QualifiedTable
	GetSourceTable() common.QualifiedTable
	// get columns and values for the record
	GetItems() T
	PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts)
}

type BaseRecord struct {
	// CheckpointID is the ID of the record.
	CheckpointID int64 `json:"checkpointId"`
	// BeginMessage.CommitTime.UnixNano(), 16 bytes smaller than time.Time
	CommitTimeNano int64 `json:"commitTimeNano"`
	// TransactionID is the `XID` corresponding to the transaction that committed this record.
	TransactionID uint64 `json:"transactionId"`
}

func (r *BaseRecord) GetCheckpointID() int64 {
	return r.CheckpointID
}

func (r *BaseRecord) GetCommitTime() time.Time {
	return time.Unix(0, r.CommitTimeNano)
}

func (r *BaseRecord) GetTransactionID() uint64 {
	return r.TransactionID
}

type InsertRecord[T Items] struct {
	// Items is a map of column name to value.
	Items T
	// Name of the source table
	SourceTable common.QualifiedTable
	// Name of the destination table
	DestinationTable common.QualifiedTable
	// CommitID is the ID of the commit corresponding to this record.
	CommitID int64
	BaseRecord
}

func (*InsertRecord[T]) Kind() string {
	return "insert"
}

func (r *InsertRecord[T]) GetDestinationTable() common.QualifiedTable {
	return r.DestinationTable
}

func (r *InsertRecord[T]) GetSourceTable() common.QualifiedTable {
	return r.SourceTable
}

func (r *InsertRecord[T]) GetItems() T {
	return r.Items
}

func (r *InsertRecord[T]) PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTable]
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
	SourceTable common.QualifiedTable
	// Name of the destination table
	DestinationTable common.QualifiedTable
	BaseRecord
}

func (*UpdateRecord[T]) Kind() string {
	return "update"
}

func (r *UpdateRecord[T]) GetDestinationTable() common.QualifiedTable {
	return r.DestinationTable
}

func (r *UpdateRecord[T]) GetSourceTable() common.QualifiedTable {
	return r.SourceTable
}

func (r *UpdateRecord[T]) GetItems() T {
	return r.NewItems
}

func (r *UpdateRecord[T]) PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTable]
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
	SourceTable common.QualifiedTable
	// Name of the destination table
	DestinationTable common.QualifiedTable
	BaseRecord
}

func (*DeleteRecord[T]) Kind() string {
	return "delete"
}

func (r *DeleteRecord[T]) GetDestinationTable() common.QualifiedTable {
	return r.DestinationTable
}

func (r *DeleteRecord[T]) GetSourceTable() common.QualifiedTable {
	return r.SourceTable
}

func (r *DeleteRecord[T]) GetItems() T {
	return r.Items
}

func (r *DeleteRecord[T]) PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts) {
	recordCount, ok := mapOfCounts[r.DestinationTable]
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

func (r *RelationRecord[T]) GetDestinationTable() common.QualifiedTable {
	dst := r.TableSchemaDelta.DstTable
	if dst == nil {
		return common.QualifiedTable{}
	}
	return common.QualifiedTable{Namespace: dst.Namespace, Table: dst.Table}
}

func (r *RelationRecord[T]) GetSourceTable() common.QualifiedTable {
	src := r.TableSchemaDelta.SrcTable
	if src == nil {
		return common.QualifiedTable{}
	}
	return common.QualifiedTable{Namespace: src.Namespace, Table: src.Table}
}

func (r *RelationRecord[T]) GetItems() T {
	var none T
	return none
}

func (r *RelationRecord[T]) PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts) {
}

type MessageRecord[T Items] struct {
	Prefix  string
	Content string
	BaseRecord
}

func (*MessageRecord[T]) Kind() string {
	return "message"
}

func (r *MessageRecord[T]) GetDestinationTable() common.QualifiedTable {
	return common.QualifiedTable{}
}

func (r *MessageRecord[T]) GetSourceTable() common.QualifiedTable {
	return common.QualifiedTable{}
}

func (r *MessageRecord[T]) GetItems() T {
	var none T
	return none
}

func (r *MessageRecord[T]) PopulateCountMap(mapOfCounts map[common.QualifiedTable]*RecordTypeCounts) {
}
