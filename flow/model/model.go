package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
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
	// LastSyncedID is the last ID that was synced.
	LastSyncState *protos.LastSyncState
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
	//relId to name Mapping
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
		// create a slice of 32 qvalues so that we don't have to allocate memory
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

func (r *RecordItems) toMap() (map[string]interface{}, error) {
	if r.colToValIdx == nil {
		return nil, errors.New("colToValIdx is nil")
	}

	jsonStruct := make(map[string]interface{})
	for col, idx := range r.colToValIdx {
		v := r.values[idx]
		if v.Value == nil {
			jsonStruct[col] = nil
			continue
		}

		var err error
		switch v.Kind {
		case qvalue.QValueKindString, qvalue.QValueKindJSON:
			strVal, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string value for column %s for %T", col, v.Value)
			}

			if len(strVal) > 15*1024*1024 {
				jsonStruct[col] = ""
			} else {
				jsonStruct[col] = strVal
			}
		case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindDate,
			qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
			jsonStruct[col], err = v.GoTimeConvert()
			if err != nil {
				return nil, err
			}
		case qvalue.QValueKindNumeric:
			bigRat, ok := v.Value.(*big.Rat)
			if !ok {
				return nil, errors.New("expected *big.Rat value")
			}
			jsonStruct[col] = bigRat.FloatString(9)
		default:
			jsonStruct[col] = v.Value
		}
	}

	return jsonStruct, nil
}

type ToJSONOptions struct {
	UnnestColumns map[string]bool
}

func NewToJSONOptions(unnestCols []string) *ToJSONOptions {
	unnestColumns := make(map[string]bool)
	for _, col := range unnestCols {
		unnestColumns[col] = true
	}
	return &ToJSONOptions{
		UnnestColumns: unnestColumns,
	}
}

func (r *RecordItems) ToJSONWithOpts(opts *ToJSONOptions) (string, error) {
	jsonStruct, err := r.toMap()
	if err != nil {
		return "", err
	}

	for col, idx := range r.colToValIdx {
		v := r.values[idx]
		if v.Kind == qvalue.QValueKindJSON {
			if _, ok := opts.UnnestColumns[col]; ok {
				var unnestStruct map[string]interface{}
				err := json.Unmarshal([]byte(v.Value.(string)), &unnestStruct)
				if err != nil {
					return "", err
				}

				for k, v := range unnestStruct {
					jsonStruct[k] = v
				}
				delete(jsonStruct, col)
			}
		}
	}

	jsonBytes, err := json.Marshal(jsonStruct)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (r *RecordItems) ToJSON() (string, error) {
	unnestCols := make([]string, 0)
	return r.ToJSONWithOpts(NewToJSONOptions(unnestCols))
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
	UnchangedToastColumns map[string]struct{}
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
	PkeyColVal string
}

type CDCRecordStream struct {
	// Records are a list of json objects.
	records chan Record
	// Schema changes from the slot
	SchemaDeltas chan *protos.TableSchemaDelta
	// Relation message mapping
	RelationMessageMapping chan *RelationMessageMapping
	// Mutex for synchronizing access to the checkpoint fields
	checkpointMutex sync.Mutex
	// firstCheckPointID is the first ID of the commit that corresponds to this batch.
	firstCheckPointID int64
	// Indicates if the last checkpoint has been set.
	lastCheckpointSet bool
	// lastCheckPointID is the last ID of the commit that corresponds to this batch.
	lastCheckPointID int64
	// empty signal to indicate if the records are going to be empty or not.
	emptySignal chan bool
}

func NewCDCRecordStream() *CDCRecordStream {
	return &CDCRecordStream{
		records: make(chan Record, 1<<18),
		// TODO (kaushik): more than 1024 schema deltas can cause problems!
		SchemaDeltas:           make(chan *protos.TableSchemaDelta, 1<<10),
		emptySignal:            make(chan bool, 1),
		RelationMessageMapping: make(chan *RelationMessageMapping, 1),
		lastCheckpointSet:      false,
		lastCheckPointID:       0,
		firstCheckPointID:      0,
	}
}

func (r *CDCRecordStream) UpdateLatestCheckpoint(val int64) {
	r.checkpointMutex.Lock()
	defer r.checkpointMutex.Unlock()

	if r.firstCheckPointID == 0 {
		r.firstCheckPointID = val
	}

	if val > r.lastCheckPointID {
		r.lastCheckPointID = val
	}
}

func (r *CDCRecordStream) GetFirstCheckpoint() int64 {
	r.checkpointMutex.Lock()
	defer r.checkpointMutex.Unlock()

	return r.firstCheckPointID
}

func (r *CDCRecordStream) GetLastCheckpoint() (int64, error) {
	r.checkpointMutex.Lock()
	defer r.checkpointMutex.Unlock()

	if !r.lastCheckpointSet {
		return 0, errors.New("last checkpoint not set, stream is still active")
	}
	return r.lastCheckPointID, nil
}

func (r *CDCRecordStream) AddRecord(record Record) {
	r.records <- record
}

func (r *CDCRecordStream) SignalAsEmpty() {
	r.emptySignal <- true
}

func (r *CDCRecordStream) SignalAsNotEmpty() {
	r.emptySignal <- false
}

func (r *CDCRecordStream) WaitAndCheckEmpty() bool {
	isEmpty := <-r.emptySignal
	return isEmpty
}

func (r *CDCRecordStream) WaitForSchemaDeltas(tableMappings []*protos.TableMapping) []*protos.TableSchemaDelta {
	schemaDeltas := make([]*protos.TableSchemaDelta, 0)
schemaLoop:
	for delta := range r.SchemaDeltas {
		for _, tm := range tableMappings {
			if delta.SrcTableName == tm.SourceTableIdentifier && delta.DstTableName == tm.DestinationTableIdentifier {
				if len(tm.Exclude) == 0 {
					break
				}
				added := make([]*protos.DeltaAddedColumn, 0, len(delta.AddedColumns))
				for _, column := range delta.AddedColumns {
					if !slices.Contains(tm.Exclude, column.ColumnName) {
						added = append(added, column)
					}
				}
				if len(added) != 0 {
					schemaDeltas = append(schemaDeltas, &protos.TableSchemaDelta{
						SrcTableName: delta.SrcTableName,
						DstTableName: delta.DstTableName,
						AddedColumns: added,
					})
				}
				continue schemaLoop
			}
		}
		schemaDeltas = append(schemaDeltas, delta)
	}
	return schemaDeltas
}

func (r *CDCRecordStream) Close() {
	close(r.emptySignal)
	close(r.records)
	close(r.SchemaDeltas)
	close(r.RelationMessageMapping)
	r.lastCheckpointSet = true
}

func (r *CDCRecordStream) GetRecords() chan Record {
	return r.records
}

type SyncRecordsRequest struct {
	Records *CDCRecordStream
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
	FlowJobName       string
	SoftDelete        bool
	SoftDeleteColName string
	SyncedAtColName   string
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
	// to be carried to parent WorkFlow
	TableSchemaDeltas []*protos.TableSchemaDelta
	// to be stored in state for future PullFlows
	RelationMessageMapping *RelationMessageMapping
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
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
