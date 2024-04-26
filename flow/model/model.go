package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

type NameAndExclude struct {
	Exclude map[string]struct{}
	Name    string
}

func NewNameAndExclude(name string, exclude []string) NameAndExclude {
	var exset map[string]struct{}
	if len(exclude) != 0 {
		exset = make(map[string]struct{}, len(exclude))
		for _, col := range exclude {
			exset[col] = struct{}{}
		}
	}
	return NameAndExclude{Name: name, Exclude: exset}
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
	TableNameMapping map[string]NameAndExclude
	// tablename to schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
	// override publication name
	OverridePublicationName string
	// override replication slot name
	OverrideReplicationSlotName string
	// LastOffset is the latest LSN that was synced.
	LastOffset int64
	// MaxBatchSize is the max number of records to fetch.
	MaxBatchSize uint32
	// IdleTimeout is the timeout to wait for new records.
	IdleTimeout time.Duration
}

type ToJSONOptions struct {
	UnnestColumns map[string]struct{}
	HStoreAsJSON  bool
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

func (t *TableWithPkey) String() string {
	return fmt.Sprintf("%s.%s", t.TableName, hex.EncodeToString(t.PkeyColVal[:]))
}

func RecToTablePKey[T Items](
	tableNameSchemaMapping map[string]*protos.TableSchema,
	rec Record[T],
) (TableWithPkey, error) {
	tableName := rec.GetDestinationTableName()
	pkeyColsMerged := make([][]byte, 0, len(tableNameSchemaMapping[tableName].PrimaryKeyColumns))

	for _, pkeyCol := range tableNameSchemaMapping[tableName].PrimaryKeyColumns {
		pkeyColBytes, err := rec.GetItems().GetBytesByColName(pkeyCol)
		if err != nil {
			return TableWithPkey{}, fmt.Errorf("error getting pkey column value: %w", err)
		}
		pkeyColsMerged = append(pkeyColsMerged, pkeyColBytes)
	}

	return TableWithPkey{
		TableName:  tableName,
		PkeyColVal: sha256.Sum256(slices.Concat(pkeyColsMerged...)),
	}, nil
}

type SyncRecordsRequest[T Items] struct {
	Records *CDCStream[T]
	// ConsumedOffset allows destination to confirm lsn for slot
	ConsumedOffset *atomic.Int64
	// FlowJobName is the name of the flow job.
	FlowJobName string
	// Staging path for AVRO files in CDC
	StagingPath string
	// Lua script
	Script string
	// source:destination mappings
	TableMappings []*protos.TableMapping
	SyncBatchID   int64
	// destination table name -> schema mapping
	TableNameSchemaMapping map[string]*protos.TableSchema
}

type NormalizeRecordsRequest struct {
	TableNameSchemaMapping map[string]*protos.TableSchema
	FlowJobName            string
	SoftDeleteColName      string
	SyncedAtColName        string
	SyncBatchID            int64
	SoftDelete             bool
}

type SyncResponse struct {
	// TableNameRowsMapping tells how many records need to be synced to each destination table.
	TableNameRowsMapping map[string]*RecordTypeCounts
	// to be carried to parent workflow
	TableSchemaDeltas []*protos.TableSchemaDelta
	// LastSyncedCheckpointID is the last ID that was synced.
	LastSyncedCheckpointID int64
	// NumRecordsSynced is the number of records that were synced.
	NumRecordsSynced   int64
	CurrentSyncBatchID int64
}

type NormalizePayload struct {
	TableNameSchemaMapping map[string]*protos.TableSchema
	Done                   bool
	SyncBatchID            int64
}

type NormalizeResponse struct {
	// Flag to depict if normalization is done
	Done         bool
	StartBatchID int64
	EndBatchID   int64
}

type RelationMessageMapping map[uint32]*pglogrepl.RelationMessage
