package model

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

type SourceTableMapping struct {
	// StructuredIngestionConfiguration is the table's structured ingestion settings, nil when it uses none.
	StructuredIngestionConfiguration *protos.StructuredIngestionTableConfig
	Exclude                          map[string]struct{}
	Name                             string
}

func NewSourceTableMapping(name string, exclude []string) SourceTableMapping {
	var exset map[string]struct{}
	if len(exclude) != 0 {
		exset = make(map[string]struct{}, len(exclude))
		for _, col := range exclude {
			exset[col] = struct{}{}
		}
	}
	return SourceTableMapping{Name: name, Exclude: exset}
}

func NewSourceTableMappingWithStructuredIngestion(
	name string, exclude []string, structuredIngestionConfiguration *protos.StructuredIngestionTableConfig,
) SourceTableMapping {
	mapping := NewSourceTableMapping(name, exclude)
	mapping.StructuredIngestionConfiguration = structuredIngestionConfiguration
	return mapping
}

type RecordTypeCounts struct {
	InsertCount atomic.Int32
	UpdateCount atomic.Int32
	DeleteCount atomic.Int32
}

type RecordsToStreamRequest[T Items] struct {
	records                  <-chan Record[T]
	TableMapping             map[string]*RecordTypeCounts
	BatchID                  int64
	UnboundedNumericAsString bool
	TargetDWH                protos.DBType
}

func NewRecordsToStreamRequest[T Items](
	records <-chan Record[T],
	tableMapping map[string]*RecordTypeCounts,
	batchID int64,
	unboundedNumericAsString bool,
	targetDWH protos.DBType,
) *RecordsToStreamRequest[T] {
	return &RecordsToStreamRequest[T]{
		records:                  records,
		TableMapping:             tableMapping,
		BatchID:                  batchID,
		UnboundedNumericAsString: unboundedNumericAsString,
		TargetDWH:                targetDWH,
	}
}

func (r *RecordsToStreamRequest[T]) GetRecords() <-chan Record[T] {
	return r.records
}

type PullRecordsRequest[T Items] struct {
	// record batch for pushing changes into
	RecordStream *CDCStream[T]
	// ConsumedOffset can be reported as committed to reduce slot size
	ConsumedOffset *atomic.Int64
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// relId to name Mapping
	SrcTableIDNameMapping map[uint32]string
	// source to destination table name mapping
	TableNameMapping map[string]SourceTableMapping
	// tablename to schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
	// overrides dynamic configuration
	Env map[string]string
	// override publication name
	OverridePublicationName string
	// override replication slot name
	OverrideReplicationSlotName string
	// LastOffset is the latest LSN that was synced.
	LastOffset CdcCheckpoint
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// peerdb versioning to prevent breaking changes
	InternalVersion uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
}

// PullTableRecordsRequest is one table's pull request in the query-based CDC
// path (see QueryCDCPullConnector). Unlike PullRecordsRequest, there is one of
// these per source table per poll, each with its own stream.
//
//nolint:govet // keeping field comments over alignment
type PullTableRecordsRequest struct {
	// overrides dynamic configuration
	Env map[string]string
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// SourceTableIdentifier is the source table this request pulls.
	SourceTableIdentifier string
	// SourceTableMapping carries the destination table name and excluded columns.
	SourceTableMapping SourceTableMapping
	// TableSchema is the schema of the destination table.
	TableSchema *protos.TableSchema
	// StartTime and EndTime are the safe source-time window selected by the activity.
	StartTime time.Time
	EndTime   time.Time
	// SafeEndTime is the furthest source time this poll may inspect. Query-mode
	// sources use it to skip across empty bounded windows without crossing the safety lag.
	SafeEndTime time.Time
	// QueryCDCMaxQueryWindow caps the duration covered by a single pull query.
	QueryCDCMaxQueryWindow time.Duration
	// Stream is where pulled records are pushed.
	Stream *CDCStream[RecordItems]
}

// PullTableRecordsResult is returned by QueryCDCPullConnector.PullTableRecords.
type PullTableRecordsResult struct {
	// NextCursor is persisted and passed back as PullTableRecordsRequest.Cursor
	// on this table's next poll.
	NextCursor string
	// BytesProcessed is the number of bytes fetched from the source for this poll.
	BytesProcessed int64
}

func EncodeQueryCDCCursor(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func DecodeQueryCDCCursor(cursor string) (time.Time, error) {
	if cursor == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, cursor)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse query CDC table cursor %q: %w", cursor, err)
	}
	return t, nil
}

// SyncQueryCDCRequest carries one table's CDC records to
// QueryCDCSyncConnector.SyncQueryCDC, which stages them (e.g. as Avro on
// S3/GCS) under BatchID, this table's own batch sequence, independent of
// every other table's, without touching the final destination table.
//
//nolint:govet // logically grouped, fieldalignment confuses things
type SyncQueryCDCRequest struct {
	Env          map[string]string
	FlowJobName  string
	TableMapping *protos.TableMapping
	TableSchema  *protos.TableSchema
	Records      <-chan Record[RecordItems]
	Version      uint32
	Flags        []string
	SchemaDeltas []*protos.TableSchemaDelta
	// BatchID is this table's own batch sequence number for the records being
	// staged, persisted as its new synced_batch_id on success.
	BatchID           int64
	SoftDeleteColName string
}

// NormalizeQueryCDCRequest asks QueryCDCSyncConnector.NormalizeQueryCDC to
// insert batches [StartBatchID, EndBatchID], previously staged by
// SyncQueryCDC, straight into the final destination table, bypassing any
// raw-table hop.
//
//nolint:govet // logically grouped, fieldalignment confuses things
type NormalizeQueryCDCRequest struct {
	Env          map[string]string
	FlowJobName  string
	TableMapping *protos.TableMapping
	TableSchema  *protos.TableSchema
	Version      uint32
	Flags        []string
	// StartBatchID is the table's last normalized batch plus one, and EndBatchID
	// is the table's last synced batch. Both bounds are inclusive.
	StartBatchID      int64
	EndBatchID        int64
	SoftDeleteColName string
}

type ToJSONOptions struct {
	UnnestColumns        map[string]struct{}
	HStoreAsJSON         bool
	ClearValuesOverBytes int // 0 means preserve values
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

type TableWithPkey struct {
	TableName string
	// SHA256 hash of the primary key columns
	PkeyColVal [32]byte
}

func RecToTablePKey[T Items](
	tableNameSchemaMapping map[string]*protos.TableSchema,
	rec Record[T],
) (TableWithPkey, error) {
	tableName := rec.GetDestinationTableName()
	hasher := sha256.New()

	for _, pkeyCol := range tableNameSchemaMapping[tableName].PrimaryKeyColumns {
		pkeyColBytes, err := rec.GetItems().GetBytesByColName(pkeyCol)
		if err != nil {
			return TableWithPkey{}, exceptions.NewPrimaryKeyModifiedError(err, tableName, pkeyCol)
		}
		// cannot return an error
		_, _ = hasher.Write(pkeyColBytes)
	}

	return TableWithPkey{
		TableName:  tableName,
		PkeyColVal: [32]byte(hasher.Sum(nil)),
	}, nil
}

//nolint:govet // keeping field comments over alignment
type SyncRecordsRequest[T Items] struct {
	Records *CDCStream[T]
	// ConsumedOffset allows destination to confirm lsn for slot
	ConsumedOffset *atomic.Int64
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// destination table name -> schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
	Env                    map[string]string
	// Staging path for AVRO files in CDC
	StagingPath string
	// Lua script
	Script string
	// source:destination mappings
	TableMappings []*protos.TableMapping
	SyncBatchID   int64
	Version       uint32
	Flags         []string
}
type NormalizeRecordsRequest struct {
	Env                    map[string]string
	TableNameSchemaMapping map[string]*protos.TableSchema
	Flags                  []string
	FlowJobName            string
	SoftDeleteColName      string
	SyncedAtColName        string
	TableMappings          []*protos.TableMapping
	SyncBatchID            int64
	Version                uint32
}

//nolint:govet // no need to save on fieldalignment
type SyncResponse struct {
	// TableNameRowsMapping tells how many records need to be synced to each destination table.
	TableNameRowsMapping map[string]*RecordTypeCounts
	// to be carried to parent workflow
	TableSchemaDeltas []*protos.TableSchemaDelta
	// LastSyncedCheckpoint is the last state (eg LSN, GTID) that was synced.
	LastSyncedCheckpoint CdcCheckpoint
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced   int64
	CurrentSyncBatchID int64
	Warnings           shared.QRepWarnings
}

type NormalizeResponse struct {
	StartBatchID int64
	EndBatchID   int64
}

type RelationMessageMapping map[uint32]*pglogrepl.RelationMessage

type SyncCompositeResponse struct {
	SyncResponse   *SyncResponse
	NeedsNormalize bool
}

type SetupReplicationResult struct {
	Conn             interface{ Close(context.Context) error }
	SlotName         string
	SnapshotName     string
	SupportsTIDScans bool
}

type RemoveFlowDetailsFromCatalogRequest struct {
	FlowName string
	Resync   bool
}
