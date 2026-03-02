package connpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	jsoniter "github.com/json-iterator/go"
	"github.com/pgvector/pgvector-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/log"

	connmetadata "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	geo "github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type PostgresCDCSource struct {
	*PostgresConnector
	srcTableIDNameMapping  map[uint32]string
	schemaNameForRelID     map[uint32]string
	tableNameMapping       map[string]model.NameAndExclude
	tableNameSchemaMapping map[string]*protos.TableSchema
	relationMessageMapping model.RelationMessageMapping
	slot                   string
	publication            string
	commitLock             *pglogrepl.BeginMessage

	// for partitioned tables, maps child relid to parent relid
	childToParentRelIDMapping map[uint32]uint32

	// for storing schema delta audit logs to catalog
	catalogPool                              shared.CatalogPool
	otelManager                              *otel_metrics.OtelManager
	hushWarnUnhandledMessageType             map[pglogrepl.MessageType]struct{}
	hushWarnUnknownTableDetected             map[uint32]struct{}
	jsonApi                                  jsoniter.API
	flowJobName                              string
	handleInheritanceForNonPartitionedTables bool
	originMetadataAsDestinationColumn        bool
	internalVersion                          uint32
}

type PostgresCDCConfig struct {
	CatalogPool                              shared.CatalogPool
	OtelManager                              *otel_metrics.OtelManager
	SrcTableIDNameMapping                    map[uint32]string
	TableNameMapping                         map[string]model.NameAndExclude
	TableNameSchemaMapping                   map[string]*protos.TableSchema
	RelationMessageMapping                   model.RelationMessageMapping
	FlowJobName                              string
	Slot                                     string
	Publication                              string
	HandleInheritanceForNonPartitionedTables bool
	SourceSchemaAsDestinationColumn          bool
	OriginMetaAsDestinationColumn            bool
	InternalVersion                          uint32
}

// Create a new PostgresCDCSource
func (c *PostgresConnector) NewPostgresCDCSource(ctx context.Context, cdcConfig *PostgresCDCConfig) (*PostgresCDCSource, error) {
	childToParentRelIDMap, err := getChildToParentRelIDMap(ctx,
		c.conn, slices.Collect(maps.Keys(cdcConfig.SrcTableIDNameMapping)),
		cdcConfig.HandleInheritanceForNonPartitionedTables)
	if err != nil {
		return nil, fmt.Errorf("error getting child to parent relid map: %w", err)
	}

	var schemaNameForRelID map[uint32]string
	if cdcConfig.SourceSchemaAsDestinationColumn {
		schemaNameForRelID = make(map[uint32]string, len(cdcConfig.TableNameSchemaMapping))
	}

	jsonApi := createExtendedJSONUnmarshaler()

	return &PostgresCDCSource{
		PostgresConnector:                        c,
		srcTableIDNameMapping:                    cdcConfig.SrcTableIDNameMapping,
		schemaNameForRelID:                       schemaNameForRelID,
		tableNameMapping:                         cdcConfig.TableNameMapping,
		tableNameSchemaMapping:                   cdcConfig.TableNameSchemaMapping,
		relationMessageMapping:                   cdcConfig.RelationMessageMapping,
		slot:                                     cdcConfig.Slot,
		publication:                              cdcConfig.Publication,
		commitLock:                               nil,
		childToParentRelIDMapping:                childToParentRelIDMap,
		catalogPool:                              cdcConfig.CatalogPool,
		otelManager:                              cdcConfig.OtelManager,
		hushWarnUnhandledMessageType:             make(map[pglogrepl.MessageType]struct{}),
		hushWarnUnknownTableDetected:             make(map[uint32]struct{}),
		jsonApi:                                  jsonApi,
		flowJobName:                              cdcConfig.FlowJobName,
		handleInheritanceForNonPartitionedTables: cdcConfig.HandleInheritanceForNonPartitionedTables,
		originMetadataAsDestinationColumn:        cdcConfig.OriginMetaAsDestinationColumn,
		internalVersion:                          cdcConfig.InternalVersion,
	}, nil
}

func (p *PostgresCDCSource) getSourceSchemaForDestinationColumn(relID uint32, tableName string) (string, error) {
	if p.schemaNameForRelID == nil {
		return "", nil
	} else if schema, ok := p.schemaNameForRelID[relID]; ok {
		return schema, nil
	}

	schemaTable, err := common.ParseTableIdentifier(tableName)
	if err != nil {
		return "", err
	}
	p.schemaNameForRelID[relID] = schemaTable.Namespace
	return schemaTable.Namespace, nil
}

func getChildToParentRelIDMap(ctx context.Context,
	conn *pgx.Conn, parentTableOIDs []uint32, handleInheritanceForNonPartitionedTables bool,
) (map[uint32]uint32, error) {
	relkinds := "'p'"
	if handleInheritanceForNonPartitionedTables {
		relkinds = "'p', 'r'"
	}

	query := fmt.Sprintf(`
		SELECT parent.oid AS parentrelid, child.oid AS childrelid
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		WHERE parent.relkind IN (%s) AND parent.oid=ANY($1);
	`, relkinds)

	rows, err := conn.Query(ctx, query, parentTableOIDs)
	if err != nil {
		return nil, fmt.Errorf("error querying for child to parent relid map: %w", err)
	}

	childToParentRelIDMap := make(map[uint32]uint32)
	var parentRelID, childRelID pgtype.Uint32
	if _, err := pgx.ForEachRow(rows, []any{&parentRelID, &childRelID}, func() error {
		childToParentRelIDMap[childRelID.Uint32] = parentRelID.Uint32
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error iterating over child to parent relid map: %w", err)
	}

	return childToParentRelIDMap, nil
}

// replProcessor implements ingesting PostgreSQL logical replication tuples into items.
type replProcessor[Items model.Items] interface {
	NewItems(int) Items

	Process(
		items Items,
		p *PostgresCDCSource,
		tuple *pglogrepl.TupleDataColumn,
		col *pglogrepl.RelationMessageColumn,
		customTypeMapping map[uint32]shared.CustomDataType,
	) error

	AddStringColumn(items Items, name string, value string)
}

type pgProcessor struct{}

func (pgProcessor) NewItems(size int) model.PgItems {
	return model.NewPgItems(size)
}

func (pgProcessor) Process(
	items model.PgItems,
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleDataColumn,
	col *pglogrepl.RelationMessageColumn,
	customTypeMapping map[uint32]shared.CustomDataType,
) error {
	switch tuple.DataType {
	case 'n': // null
		items.AddColumn(col.Name, nil)
	case 't': // text
		// bytea also appears here as a hex
		items.AddColumn(col.Name, tuple.Data)
	case 'b': // binary
		return fmt.Errorf(
			"binary encoding not supported, received for %s type %d",
			col.Name,
			col.DataType,
		)
	default:
		return fmt.Errorf("unknown column data type: %s", string(tuple.DataType))
	}
	return nil
}

func (pgProcessor) AddStringColumn(items model.PgItems, name string, value string) {
	items.AddColumn(name, shared.UnsafeFastStringToReadOnlyBytes(value))
}

type qProcessor struct{}

func (qProcessor) NewItems(size int) model.RecordItems {
	return model.NewRecordItems(size)
}

func (qProcessor) Process(
	items model.RecordItems,
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleDataColumn,
	col *pglogrepl.RelationMessageColumn,
	customTypeMapping map[uint32]shared.CustomDataType,
) error {
	switch tuple.DataType {
	case 'n': // null
		items.AddColumn(col.Name, types.QValueNull(types.QValueKindInvalid))
	case 't': // text
		// bytea also appears here as a hex
		data, err := p.decodeColumnData(
			tuple.Data, col.DataType, col.TypeModifier, pgtype.TextFormatCode, customTypeMapping, p.internalVersion,
		)
		if err != nil {
			p.logger.Error("error decoding text column data", slog.Any("error", err),
				slog.String("columnName", col.Name), slog.Uint64("dataType", uint64(col.DataType)))
			return fmt.Errorf("error decoding text column data: %w", err)
		}
		items.AddColumn(col.Name, data)
	case 'b': // binary
		data, err := p.decodeColumnData(
			tuple.Data, col.DataType, col.TypeModifier, pgtype.BinaryFormatCode, customTypeMapping, p.internalVersion,
		)
		if err != nil {
			return fmt.Errorf("error decoding binary column data: %w", err)
		}
		items.AddColumn(col.Name, data)
	default:
		return fmt.Errorf("unknown column data type: %s", string(tuple.DataType))
	}
	return nil
}

func (qProcessor) AddStringColumn(items model.RecordItems, name string, value string) {
	items.AddColumn(name, types.QValueString{Val: value})
}

func processTuple[Items model.Items](
	processor replProcessor[Items],
	p *PostgresCDCSource,
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessage,
	nameAndExclude model.NameAndExclude,
	customTypeMapping map[uint32]shared.CustomDataType,
	schemaName string,
	baseRecord model.BaseRecord,
) (Items, map[string]struct{}, error) {
	// if the tuple is nil, return an empty map
	if tuple == nil {
		return processor.NewItems(0), nil, nil
	}

	items := processor.NewItems(len(tuple.Columns))
	var unchangedToastColumns map[string]struct{}

	var none Items
	if len(tuple.Columns) > len(rel.Columns) {
		return none, nil, fmt.Errorf(
			"tuple has more columns than the last RelationMessage: %d > %d. "+
				"One known occurrence of this was due to a bug with replication column lists in PG 15-15.1. "+
				"https://www.postgresql.org/message-id/CADGJaX9kiRZ-OH0EpWF5Fkyh1ZZYofoNRCrhapBfdk02tj5EKg@mail.gmail.com",
			len(tuple.Columns), len(rel.Columns))
	}

	for idx, tcol := range tuple.Columns {
		rcol := rel.Columns[idx]
		if _, ok := nameAndExclude.Exclude[rcol.Name]; ok {
			continue
		}
		if tcol.DataType == 'u' {
			if unchangedToastColumns == nil {
				unchangedToastColumns = make(map[string]struct{})
			}
			unchangedToastColumns[rcol.Name] = struct{}{}
		} else if err := processor.Process(items, p, tcol, rcol, customTypeMapping); err != nil {
			return none, nil, err
		}
	}

	if schemaName != "" {
		processor.AddStringColumn(items, "_peerdb_source_schema", schemaName)
	}

	if p.originMetadataAsDestinationColumn {
		items.UpdateWithBaseRecord(baseRecord)
	}

	return items, unchangedToastColumns, nil
}

func (p *PostgresCDCSource) decodeColumnData(
	data []byte, dataType uint32, typmod int32, formatCode int16, customTypeMapping map[uint32]shared.CustomDataType, version uint32,
) (types.QValue, error) {
	var parsedData any
	var err error

	// Special handling for JSON types to use relaxed number parsing
	if dataType == pgtype.JSONOID || dataType == pgtype.JSONBOID {
		var text pgtype.Text
		if err := p.typeMap.Scan(dataType, formatCode, data, &text); err != nil {
			p.logger.Error("[pg_cdc] failed to scan json", slog.Any("error", err))
			return nil, fmt.Errorf("failed to scan json: %w", err)
		}
		if text.Valid {
			if err := p.jsonApi.UnmarshalFromString(text.String, &parsedData); err != nil {
				p.logger.Error("[pg_cdc] failed to unmarshal json", slog.Any("error", err))
				return nil, fmt.Errorf("failed to unmarshal json: %w", err)
			}
			if parsedData == nil {
				// avoid confusing SQL null & JSON null by using pre-marshaled value
				parsedData = json.RawMessage("null")
			}
			return p.parseFieldFromPostgresOID(dataType, typmod, true, protos.DBType_DBTYPE_UNKNOWN,
				parsedData, customTypeMapping, p.internalVersion)
		}
		return types.QValueNull(types.QValueKindJSON), nil
	} else if dataType == pgtype.JSONArrayOID || dataType == pgtype.JSONBArrayOID {
		textArr := pgtype.FlatArray[pgtype.Text]{}
		if err := p.typeMap.Scan(dataType, formatCode, data, &textArr); err != nil {
			p.logger.Error("[pg_cdc] failed to scan json array", slog.Any("error", err))
			return nil, fmt.Errorf("failed to scan json array: %w", err)
		}

		arr := make([]any, len(textArr))
		for j, text := range textArr {
			if text.Valid {
				if err := p.jsonApi.UnmarshalFromString(text.String, &arr[j]); err != nil {
					p.logger.Error("[pg_cdc] failed to unmarshal json array element", slog.Any("error", err))
					return nil, fmt.Errorf("failed to unmarshal json array element: %w", err)
				}
			} else {
				arr[j] = nil
			}
		}
		return p.parseFieldFromPostgresOID(dataType, typmod, true, protos.DBType_DBTYPE_UNKNOWN, arr, customTypeMapping, p.internalVersion)
	} else if dt, ok := p.typeMap.TypeForOID(dataType); ok {
		dtOid := dt.OID
		if dtOid == pgtype.CIDROID || dtOid == pgtype.InetOID || dtOid == pgtype.MacaddrOID || dtOid == pgtype.XMLOID {
			// below is required to decode above types to string
			parsedData, err = dt.Codec.DecodeDatabaseSQLValue(p.typeMap, dataType, formatCode, data)
		} else {
			parsedData, err = dt.Codec.DecodeValue(p.typeMap, dataType, formatCode, data)
		}
		if err != nil {
			if dtOid == pgtype.TimeOID || dtOid == pgtype.TimetzOID ||
				dtOid == pgtype.TimestampOID || dtOid == pgtype.TimestamptzOID {
				// indicates year is more than 4 digits or something similar,
				// which you can insert into postgres, but not representable by time.Time
				p.logger.Warn("Invalidate time for destination, nulled", slog.String("typeName", dt.Name), slog.String("value", string(data)))
				switch dtOid {
				case pgtype.TimeOID:
					return types.QValueNull(types.QValueKindTime), nil
				case pgtype.TimetzOID:
					return types.QValueNull(types.QValueKindTimeTZ), nil
				case pgtype.TimestampOID:
					return types.QValueNull(types.QValueKindTimestamp), nil
				case pgtype.TimestamptzOID:
					return types.QValueNull(types.QValueKindTimestampTZ), nil
				}
			}
			return nil, err
		}
		return p.parseFieldFromPostgresOID(dataType, typmod, true, protos.DBType_DBTYPE_UNKNOWN,
			parsedData, customTypeMapping, p.internalVersion)
	} else if dataType == pgtype.TimetzOID { // ugly TIMETZ workaround for CDC decoding.
		return p.parseFieldFromPostgresOID(dataType, typmod, true, protos.DBType_DBTYPE_UNKNOWN,
			string(data), customTypeMapping, p.internalVersion)
	} else if typeData, ok := customTypeMapping[dataType]; ok {
		customQKind := CustomTypeToQKind(typeData, version)
		switch customQKind {
		case types.QValueKindGeography, types.QValueKindGeometry:
			wkt, err := geo.GeoValidate(string(data))
			if err != nil {
				p.logger.Warn("failure during GeoValidate", slog.Any("error", err))
				return types.QValueNull(customQKind), nil
			} else if customQKind == types.QValueKindGeography {
				return types.QValueGeography{Val: wkt}, nil
			} else {
				return types.QValueGeometry{Val: wkt}, nil
			}
		case types.QValueKindHStore:
			return types.QValueHStore{Val: string(data)}, nil
		case types.QValueKindString:
			return types.QValueString{Val: string(data)}, nil
		case types.QValueKindEnum:
			return types.QValueEnum{Val: string(data)}, nil
		case types.QValueKindArrayString:
			return types.QValueArrayString{Val: shared.ParsePgArrayToStringSlice(data, typeData.Delim)}, nil
		case types.QValueKindArrayFloat32:
			switch typeData.Name {
			case "vector":
				var vector pgvector.Vector
				if err := vector.Parse(string(data)); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse vector: %w", err)
				}
				return types.QValueArrayFloat32{Val: vector.Slice()}, nil
			case "halfvec":
				var halfvec pgvector.HalfVector
				if err := halfvec.Parse(string(data)); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse halfvec: %w", err)
				}
				return types.QValueArrayFloat32{Val: halfvec.Slice()}, nil
			case "sparsevec":
				var sparsevec pgvector.SparseVector
				if err := sparsevec.Parse(string(data)); err != nil {
					return nil, fmt.Errorf("[pg] failed to parse sparsevec: %w", err)
				}
				return types.QValueArrayFloat32{Val: sparsevec.Slice()}, nil
			default:
				return nil, fmt.Errorf("unknown float array type %s", typeData.Name)
			}
		case types.QValueKindArrayEnum:
			return types.QValueArrayEnum{Val: shared.ParsePgArrayToStringSlice(data, typeData.Delim)}, nil
		default:
			return nil, fmt.Errorf("unknown custom qkind for %s: %s", typeData.Name, customQKind)
		}
	}

	return types.QValueString{Val: string(data)}, nil
}

// PullCdcRecords pulls records from req's cdc stream
func PullCdcRecords[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	req *model.PullRecordsRequest[Items],
	processor replProcessor[Items],
	replLock *sync.Mutex,
) error {
	logger := internal.LoggerFromCtx(ctx)

	// use only with taking replLock
	conn := p.replConn.PgConn()
	sendStandbyAfterReplLock := func(updateType string) error {
		replLock.Lock()
		defer replLock.Unlock()
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(req.ConsumedOffset.Load())},
		); err != nil {
			return fmt.Errorf("[%s] SendStandbyStatusUpdate failed: %w", updateType, err)
		}
		return nil
	}

	records := req.RecordStream
	var totalRecords int64
	var fetchedBytes, totalFetchedBytes, allFetchedBytes atomic.Int64
	// clientXLogPos is the last checkpoint id, we need to ack that we have processed
	// until clientXLogPos each time we send a standby status update.
	var clientXLogPos pglogrepl.LSN
	if req.LastOffset.ID > 0 {
		clientXLogPos = pglogrepl.LSN(req.LastOffset.ID)
		if err := sendStandbyAfterReplLock("initial-flush"); err != nil {
			return err
		}
	}

	// Remove exceptions.PrimaryKeyModifiedError and its classification when cdc store is removed
	cdcStoreEnabled, err := internal.PeerDBCDCStoreEnabled(ctx, req.Env)
	if err != nil {
		return err
	}
	var cdcRecordsStorage *utils.CDCStore[Items]
	if cdcStoreEnabled {
		cdcRecordsStorage, err = utils.NewCDCStore[Items](ctx, req.Env, p.flowJobName)
		if err != nil {
			return err
		}
	}

	pullStart := time.Now()
	defer func() {
		if totalRecords == 0 {
			records.SignalAsEmpty()
		}
		logger.Info("[finished] PullRecords",
			slog.Int64("records", totalRecords),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", records.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
		if cdcRecordsStorage != nil {
			if err := cdcRecordsStorage.Close(); err != nil {
				logger.Warn("failed to clean up records storage", slog.Any("error", err))
			}
		}
	}()

	logger.Info("pulling records start")

	waitingForCommit := false
	defer func() {
		p.otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		p.otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, allFetchedBytes.Swap(0))
	}()
	shutdown := common.Interval(ctx, time.Minute, func() {
		p.otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		p.otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, allFetchedBytes.Swap(0))
		logger.Info("pulling records",
			slog.Int64("records", totalRecords),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", records.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()),
			slog.Bool("waitingForCommit", waitingForCommit))
	})
	defer shutdown()

	var standByLastLogged time.Time
	nextStandbyMessageDeadline := time.Now().Add(req.IdleTimeout)
	pkmRequiresResponse := false

	addRecordWithKey := func(key model.TableWithPkey, rec model.Record[Items]) error {
		if cdcRecordsStorage != nil {
			if err := cdcRecordsStorage.Set(key, rec); err != nil {
				return err
			}
		}
		if err := records.AddRecord(ctx, rec); err != nil {
			return err
		}

		totalRecords++

		if totalRecords == 1 {
			records.SignalAsNotEmpty()
			nextStandbyMessageDeadline = time.Now().Add(req.IdleTimeout)
			logger.Info(fmt.Sprintf("pushing the standby deadline to %s", nextStandbyMessageDeadline))
		}
		if totalRecords%50000 == 0 {
			logger.Info("pulling records",
				slog.Int64("records", totalRecords),
				slog.Int64("bytes", totalFetchedBytes.Load()),
				slog.Int("channelLen", records.ChannelLen()),
				slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()),
				slog.Bool("waitingForCommit", waitingForCommit))
		}
		return nil
	}

	pkmEmptyBatchThrottleThresholdSeconds, err := internal.PeerDBPKMEmptyBatchThrottleThresholdSeconds(ctx, req.Env)
	if err != nil {
		logger.Error("failed to get PeerDBPKMEmptyBatchThrottleThresholdSeconds", slog.Any("error", err))
	}
	lastEmptyBatchPkmSentTime := time.Now()
	for {
		if pkmRequiresResponse {
			if totalRecords == 0 && int64(clientXLogPos) > req.ConsumedOffset.Load() {
				err := p.updateConsumedOffset(ctx, logger, req.FlowJobName, req.ConsumedOffset, clientXLogPos)
				if err != nil {
					return err
				}
				lastEmptyBatchPkmSentTime = time.Now()
			}

			if err := sendStandbyAfterReplLock("pkm-response"); err != nil {
				return err
			}
			pkmRequiresResponse = false

			if time.Since(standByLastLogged) > 10*time.Second {
				logger.Info("Sent Standby status message",
					slog.Int64("records", totalRecords),
					slog.Int64("bytes", totalFetchedBytes.Load()),
					slog.Int("channelLen", records.ChannelLen()),
					slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()),
					slog.Bool("waitingForCommit", waitingForCommit))
				standByLastLogged = time.Now()
			}
		}

		if p.commitLock == nil {
			if totalRecords >= int64(req.MaxBatchSize) {
				logger.Info("batch filled, returning currently accumulated records",
					slog.Int64("records", totalRecords),
					slog.Int64("bytes", totalFetchedBytes.Load()),
					slog.Int("channelLen", records.ChannelLen()),
					slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
				return nil
			}

			if waitingForCommit {
				logger.Info("commit received, returning currently accumulated records",
					slog.Int64("records", totalRecords),
					slog.Int64("bytes", totalFetchedBytes.Load()),
					slog.Int("channelLen", records.ChannelLen()),
					slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
				return nil
			}
		}

		// if we are past the next standby deadline (?)
		if time.Now().After(nextStandbyMessageDeadline) {
			if totalRecords != 0 {
				logger.Info("standby deadline reached", slog.Int64("records", totalRecords))

				if p.commitLock == nil {
					logger.Info("no commit lock, returning currently accumulated records",
						slog.Int64("records", totalRecords),
						slog.Int64("bytes", totalFetchedBytes.Load()),
						slog.Int("channelLen", records.ChannelLen()),
						slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
					return nil
				} else {
					logger.Info("commit lock, waiting for commit to return records",
						slog.Int64("records", totalRecords),
						slog.Int64("bytes", totalFetchedBytes.Load()),
						slog.Int("channelLen", records.ChannelLen()),
						slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
					waitingForCommit = true
				}
			} else {
				logger.Info(("standby deadline reached, no records accumulated, continuing to wait"))
			}
			nextStandbyMessageDeadline = time.Now().Add(req.IdleTimeout)
		}

		var receiveCtx context.Context
		var cancel context.CancelFunc
		if totalRecords == 0 {
			receiveCtx, cancel = context.WithCancel(ctx)
		} else {
			receiveCtx, cancel = context.WithDeadline(ctx, nextStandbyMessageDeadline)
		}
		rawMsg, err := func() (pgproto3.BackendMessage, error) {
			replLock.Lock()
			defer replLock.Unlock()
			return conn.ReceiveMessage(receiveCtx)
		}()
		cancel()

		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("consumeStream preempted: %w", ctxErr)
		}

		if err != nil && p.commitLock == nil {
			if pgconn.Timeout(err) {
				logger.Info("Stand-by deadline reached, returning currently accumulated records",
					slog.Int64("records", totalRecords),
					slog.Int64("bytes", totalFetchedBytes.Load()),
					slog.Int("channelLen", records.ChannelLen()),
					slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
				return nil
			} else {
				return fmt.Errorf("ReceiveMessage failed: %w", err)
			}
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			return shared.LogError(logger, exceptions.NewPostgresWalError(errors.New("received error response"), msg))
		case *pgproto3.CopyData:
			allFetchedBytes.Add(int64(len(msg.Data)))
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
				}

				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}

				if pkm.ReplyRequested || (pkmEmptyBatchThrottleThresholdSeconds != -1 &&
					time.Since(lastEmptyBatchPkmSentTime) >= time.Duration(pkmEmptyBatchThrottleThresholdSeconds)*time.Second) {
					pkmRequiresResponse = true
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("ParseXLogData failed: %w", err)
				}

				logger.Debug("XLogData",
					slog.Any("WALStart", xld.WALStart), slog.Any("ServerWALEnd", xld.ServerWALEnd), slog.Time("ServerTime", xld.ServerTime))
				rec, err := processMessage(ctx, p, records, xld, clientXLogPos, processor)
				if err != nil {
					return exceptions.NewPostgresLogicalMessageProcessingError(err)
				}

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				if rec != nil {
					fetchedBytes.Add(int64(len(msg.Data)))
					totalFetchedBytes.Add(int64(len(msg.Data)))
					tableName := rec.GetDestinationTableName()
					switch r := rec.(type) {
					case *model.UpdateRecord[Items]:
						// tableName here is destination tableName.
						// should be ideally sourceTableName as we are in PullRecords.
						// will change in future
						// TODO: replident is cached here, should not cache since it can change
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							if cdcRecordsStorage != nil {
								if latestRecord, found, err := cdcRecordsStorage.Get(tablePkeyVal); err != nil {
									return err
								} else if found {
									// iterate through unchanged toast cols and set them in new record
									updatedCols := r.NewItems.UpdateIfNotExists(latestRecord.GetItems())
									for _, col := range updatedCols {
										delete(r.UnchangedToastColumns, col)
									}
									p.otelManager.Metrics.UnchangedToastValuesCounter.Add(ctx, int64(len(updatedCols)),
										metric.WithAttributeSet(attribute.NewSet(
											attribute.Bool("backfilled", true))))
								}
							}
							p.otelManager.Metrics.UnchangedToastValuesCounter.Add(ctx, int64(len(r.UnchangedToastColumns)),
								metric.WithAttributeSet(attribute.NewSet(
									attribute.Bool("backfilled", false))))

							if err := addRecordWithKey(tablePkeyVal, rec); err != nil {
								return err
							}
						}

					case *model.InsertRecord[Items]:
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							if err := addRecordWithKey(tablePkeyVal, rec); err != nil {
								return err
							}
						}
					case *model.DeleteRecord[Items]:
						isFullReplica := req.TableNameSchemaMapping[tableName].IsReplicaIdentityFull
						if isFullReplica {
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						} else {
							tablePkeyVal, err := model.RecToTablePKey(req.TableNameSchemaMapping, rec)
							if err != nil {
								return err
							}

							backfilled := false
							if cdcRecordsStorage != nil {
								if latestRecord, found, err := cdcRecordsStorage.Get(tablePkeyVal); err != nil {
									return err
								} else if found {
									r.Items = latestRecord.GetItems()
									if updateRecord, ok := latestRecord.(*model.UpdateRecord[Items]); ok {
										r.UnchangedToastColumns = updateRecord.UnchangedToastColumns
										backfilled = true
									}
								}
							}
							if !backfilled {
								// there is nothing to backfill the items in the delete record with,
								// so don't update the row with this record
								// add sentinel value to prevent update statements from selecting
								r.UnchangedToastColumns = map[string]struct{}{
									"_peerdb_not_backfilled_delete": {},
								}
							}

							// A delete can only be followed by an INSERT, which does not need backfilling
							// No need to store DeleteRecords in memory or disk.
							if err := addRecordWithKey(model.TableWithPkey{}, rec); err != nil {
								return err
							}
						}

					case *model.RelationRecord[Items]:
						tableSchemaDelta := r.TableSchemaDelta
						if len(tableSchemaDelta.AddedColumns) > 0 {
							logger.Info(fmt.Sprintf("Detected schema change for table %s, addedColumns: %v",
								tableSchemaDelta.SrcTableName, tableSchemaDelta.AddedColumns))
							records.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
						}

					case *model.MessageRecord[Items]:
						// if there were no records, we can move lsn,
						// otherwise push to records so destination can ack once all previous messages processed
						if totalRecords == 0 {
							if int64(clientXLogPos) > req.ConsumedOffset.Load() {
								if err := p.updateConsumedOffset(ctx, logger, req.FlowJobName, req.ConsumedOffset, clientXLogPos); err != nil {
									return err
								}
							}
						} else if err := records.AddRecord(ctx, rec); err != nil {
							return err
						}
					}
				}
			}
		}
	}
}

func (p *PostgresCDCSource) updateConsumedOffset(
	ctx context.Context,
	logger log.Logger,
	flowJobName string,
	consumedOffset *atomic.Int64,
	clientXLogPos pglogrepl.LSN,
) error {
	metadata := connmetadata.NewPostgresMetadataFromCatalog(logger, p.catalogPool)
	if err := metadata.SetLastOffset(ctx, flowJobName, model.CdcCheckpoint{ID: int64(clientXLogPos)}); err != nil {
		return err
	}
	consumedOffset.Store(int64(clientXLogPos))
	p.otelManager.Metrics.CommittedLSNGauge.Record(ctx, int64(clientXLogPos))
	return nil
}

func (p *PostgresCDCSource) baseRecord(lsn pglogrepl.LSN) model.BaseRecord {
	var nano int64
	var transactionID uint64
	if p.commitLock != nil {
		nano = p.commitLock.CommitTime.UnixNano()
		transactionID = uint64(p.commitLock.Xid)
	}
	return model.BaseRecord{
		CheckpointID:   int64(lsn),
		CommitTimeNano: nano,
		TransactionID:  transactionID,
	}
}

func processMessage[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	batch *model.CDCStream[Items],
	xld pglogrepl.XLogData,
	currentClientXlogPos pglogrepl.LSN,
	processor replProcessor[Items],
) (model.Record[Items], error) {
	logger := internal.LoggerFromCtx(ctx)
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}
	customTypeMapping, err := p.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		logger.Debug("BeginMessage", slog.Any("FinalLSN", msg.FinalLSN), slog.Uint64("XID", uint64(msg.Xid)))
		p.commitLock = msg
	case *pglogrepl.InsertMessage:
		return processInsertMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.UpdateMessage:
		return processUpdateMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.DeleteMessage:
		return processDeleteMessage(p, xld.WALStart, msg, processor, customTypeMapping)
	case *pglogrepl.CommitMessage:
		// for a commit message, update the last checkpoint id for the record batch.
		logger.Debug("CommitMessage",
			slog.Any("CommitLSN", msg.CommitLSN),
			slog.Any("TransactionEndLSN", msg.TransactionEndLSN))
		batch.UpdateLatestCheckpointID(int64(msg.CommitLSN))
		p.otelManager.Metrics.ReceivedCommitLSNGauge.Record(ctx, int64(msg.CommitLSN))
		p.otelManager.Metrics.CommitLagGauge.Record(ctx, time.Now().UTC().Sub(msg.CommitTime).Microseconds())
		p.commitLock = nil
	case *pglogrepl.RelationMessage:
		// For child tables (partitioned/inherited), we still process the message but store under actual relID
		// so that tuple decoding uses the correct per-relation schema. Use effective (parent) relID only for "do we care" check.
		effectiveRelID, err := p.checkIfUnknownTableInherits(ctx, msg.RelationID)
		if err != nil {
			return nil, err
		}

		if _, exists := p.srcTableIDNameMapping[effectiveRelID]; !exists {
			return nil, nil
		}

		logger.Info("processing RelationMessage",
			slog.Any("LSN", currentClientXlogPos),
			slog.Uint64("RelationID", uint64(msg.RelationID)),
			slog.String("Namespace", msg.Namespace),
			slog.String("RelationName", msg.RelationName),
			slog.Any("Columns", msg.Columns))

		return processRelationMessage[Items](ctx, p, currentClientXlogPos, msg)
	case *pglogrepl.LogicalDecodingMessage:
		logger.Debug("LogicalDecodingMessage",
			slog.Bool("Transactional", msg.Transactional),
			slog.String("Prefix", msg.Prefix),
			slog.String("LSN", msg.LSN.String()))
		if !msg.Transactional {
			batch.UpdateLatestCheckpointID(int64(msg.LSN))
		}
		return &model.MessageRecord[Items]{
			BaseRecord: p.baseRecord(msg.LSN),
			Prefix:     msg.Prefix,
			Content:    string(msg.Content),
		}, nil
	default:
		if _, ok := p.hushWarnUnhandledMessageType[msg.Type()]; !ok {
			logger.Warn(fmt.Sprintf("Unhandled message type: %T", msg))
			p.hushWarnUnhandledMessageType[msg.Type()] = struct{}{}
		}
	}

	return nil, nil
}

func processInsertMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.InsertMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	// Use actual relation (child) for decoding tuple; use parent only for table/schema mapping.
	// For inherited tables, the child can have different column types than the parent (e.g. TEXT vs UUID).
	actualRelID := msg.RelationID
	relID := p.getParentRelIDIfPartitioned(actualRelID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("InsertMessage", slog.Any("LSN", lsn), slog.Uint64("RelationID", uint64(relID)), slog.String("Relation Name", tableName))

	// Decode using the relation that produced this tuple (child), not the parent's schema.
	rel, ok := p.relationMessageMapping[actualRelID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", actualRelID)
	}

	schemaName, err := p.getSourceSchemaForDestinationColumn(relID, tableName)
	if err != nil {
		return nil, err
	}

	baseRecord := p.baseRecord(lsn)
	items, _, err := processTuple(processor, p, msg.Tuple, rel, p.tableNameMapping[tableName], customTypeMapping, schemaName, baseRecord)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.InsertRecord[Items]{
		BaseRecord:           baseRecord,
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processUpdateMessage processes an update message and returns an UpdateRecord
func processUpdateMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.UpdateMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	// Use actual relation (child) for decoding tuple; use parent only for table/schema mapping.
	actualRelID := msg.RelationID
	relID := p.getParentRelIDIfPartitioned(actualRelID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("UpdateMessage", slog.Any("LSN", lsn), slog.Uint64("RelationID", uint64(relID)), slog.String("Relation Name", tableName))

	// Decode using the relation that produced this tuple (child), not the parent's schema.
	rel, ok := p.relationMessageMapping[actualRelID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", actualRelID)
	}

	schemaName, err := p.getSourceSchemaForDestinationColumn(relID, tableName)
	if err != nil {
		return nil, err
	}

	oldItems, _, err := processTuple(processor, p, msg.OldTuple, rel, p.tableNameMapping[tableName], customTypeMapping, "", model.BaseRecord{})
	if err != nil {
		return nil, fmt.Errorf("error converting old tuple to map: %w", err)
	}

	baseRecord := p.baseRecord(lsn)
	newItems, unchangedToastColumns, err := processTuple(
		processor, p, msg.NewTuple, rel, p.tableNameMapping[tableName], customTypeMapping, schemaName, baseRecord)
	if err != nil {
		return nil, fmt.Errorf("error converting new tuple to map: %w", err)
	}

	/*
	   Looks like in some cases (at least replident full + TOAST), the new tuple doesn't contain unchanged columns
	   and only the old tuple does. So we can backfill the new tuple with the unchanged columns from the old tuple.
	   Otherwise, _peerdb_unchanged_toast_columns is set correctly and we fallback to normal unchanged TOAST handling in normalize,
	   but this doesn't work in connectors where we don't do unchanged TOAST handling in normalize.
	*/
	backfilledCols := newItems.UpdateIfNotExists(oldItems)
	for _, col := range backfilledCols {
		delete(unchangedToastColumns, col)
		// we only use _peerdb_data anyway, remove for space optimization
		oldItems.DeleteColName(col)
	}

	return &model.UpdateRecord[Items]{
		BaseRecord:            baseRecord,
		OldItems:              oldItems,
		NewItems:              newItems,
		DestinationTableName:  p.tableNameMapping[tableName].Name,
		SourceTableName:       tableName,
		UnchangedToastColumns: unchangedToastColumns,
	}, nil
}

// processDeleteMessage processes a delete message and returns a DeleteRecord
func processDeleteMessage[Items model.Items](
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	msg *pglogrepl.DeleteMessage,
	processor replProcessor[Items],
	customTypeMapping map[uint32]shared.CustomDataType,
) (model.Record[Items], error) {
	// Use actual relation (child) for decoding tuple; use parent only for table/schema mapping.
	actualRelID := msg.RelationID
	relID := p.getParentRelIDIfPartitioned(actualRelID)

	tableName, exists := p.srcTableIDNameMapping[relID]
	if !exists {
		return nil, nil
	}

	// log lsn and relation id for debugging
	p.logger.Debug("DeleteMessage", slog.Any("LSN", lsn), slog.Uint64("RelationID", uint64(relID)), slog.String("Relation Name", tableName))

	// Decode using the relation that produced this tuple (child), not the parent's schema.
	rel, ok := p.relationMessageMapping[actualRelID]
	if !ok {
		return nil, fmt.Errorf("unknown relation id: %d", actualRelID)
	}

	schemaName, err := p.getSourceSchemaForDestinationColumn(relID, tableName)
	if err != nil {
		return nil, err
	}

	baseRecord := p.baseRecord(lsn)
	items, _, err := processTuple(processor, p, msg.OldTuple, rel, p.tableNameMapping[tableName], customTypeMapping, schemaName, baseRecord)
	if err != nil {
		return nil, fmt.Errorf("error converting tuple to map: %w", err)
	}

	return &model.DeleteRecord[Items]{
		BaseRecord:           baseRecord,
		Items:                items,
		DestinationTableName: p.tableNameMapping[tableName].Name,
		SourceTableName:      tableName,
	}, nil
}

// processRelationMessage processes a RelationMessage and returns a TableSchemaDelta
func processRelationMessage[Items model.Items](
	ctx context.Context,
	p *PostgresCDCSource,
	lsn pglogrepl.LSN,
	currRel *pglogrepl.RelationMessage,
) (model.Record[Items], error) {
	// Resolve to logical (parent) table for name/schema lookups; we still store under actual relID for correct tuple decoding.
	effectiveRelID := p.getParentRelIDIfPartitioned(currRel.RelationID)
	currRelName, ok := p.srcTableIDNameMapping[effectiveRelID]
	if !ok {
		p.logger.Warn("relid not present in srcTableIDNameMapping, skipping relation message",
			slog.Uint64("relId", uint64(currRel.RelationID)))
		return nil, nil
	}

	// For child tables (partitioned/inherited), query the parent's actual columns
	// to distinguish child-specific columns from genuinely new parent columns.
	// Without this, columns unique to a child table are falsely detected as schema
	// changes on the parent, triggering spurious ALTER TABLE statements on the destination.
	isChildTable := currRel.RelationID != effectiveRelID
	var parentColumnSet map[string]struct{}
	if isChildTable {
		parentRows, err := p.conn.Query(ctx,
			"select attname from pg_attribute where attrelid=$1 and attnum > 0 and not attisdropped",
			effectiveRelID)
		if err != nil {
			return nil, fmt.Errorf("error querying parent table columns for schema delta: %w", err)
		}
		parentColumns, err := pgx.CollectRows[string](parentRows, pgx.RowTo)
		if err != nil {
			return nil, fmt.Errorf("error collecting parent table columns for schema delta: %w", err)
		}
		parentColumnSet = make(map[string]struct{}, len(parentColumns))
		for _, name := range parentColumns {
			parentColumnSet[name] = struct{}{}
		}
	}

	customTypeMapping, err := p.fetchCustomTypeMapping(ctx)
	if err != nil {
		return nil, err
	}

	// retrieve current TableSchema for table changed, mapping uses dst table name as key, need to translate source name
	currRelDstInfo, ok := p.tableNameMapping[currRelName]
	if !ok {
		p.logger.Error("Detected relation message for table, but not in table name mapping",
			slog.String("tableName", currRelName))
		return nil, fmt.Errorf("cannot find table name mapping for %s", currRelName)
	}

	prevSchema, ok := p.tableNameSchemaMapping[currRelDstInfo.Name]
	if !ok {
		p.logger.Error("Detected relation message for table, but not in table schema mapping",
			slog.String("tableName", currRelDstInfo.Name))
		return nil, fmt.Errorf("cannot find table schema for %s", currRelDstInfo.Name)
	}

	prevRelMap := make(map[string]string, len(prevSchema.Columns))
	for _, column := range prevSchema.Columns {
		prevRelMap[column.Name] = column.Type
	}

	currRelMap := make(map[string]string, len(currRel.Columns))
	for _, column := range currRel.Columns {
		switch prevSchema.System {
		case protos.TypeSystem_Q:
			qKind := p.postgresOIDToQValueKind(column.DataType, customTypeMapping, p.internalVersion)
			if qKind == types.QValueKindInvalid {
				if typeName, ok := customTypeMapping[column.DataType]; ok {
					qKind = CustomTypeToQKind(typeName, p.internalVersion)
				}
			}
			currRelMap[column.Name] = string(qKind)
		case protos.TypeSystem_PG:
			typeName, err := p.postgresOIDToName(column.DataType, customTypeMapping)
			if err != nil {
				return nil, err
			}
			currRelMap[column.Name] = typeName
		default:
			panic(fmt.Sprintf("cannot process schema changes for unknown type system %s", prevSchema.System))
		}
	}

	var potentiallyNullableAddedColumns []string
	schemaDelta := &protos.TableSchemaDelta{
		SrcTableName:    p.srcTableIDNameMapping[effectiveRelID],
		DstTableName:    p.tableNameMapping[p.srcTableIDNameMapping[effectiveRelID]].Name,
		AddedColumns:    nil,
		System:          prevSchema.System,
		NullableEnabled: prevSchema.NullableEnabled,
	}

	isAddedColumnAndNotExcluded := func(columnName string) bool {
		_, inPrevRel := prevRelMap[columnName]
		if inPrevRel {
			return false
		}
		// For child tables, only propagate columns that actually exist on the parent table.
		// Columns unique to the child should not trigger schema deltas.
		if parentColumnSet != nil {
			if _, onParent := parentColumnSet[columnName]; !onParent {
				return false
			}
		}
		_, isExcluded := p.tableNameMapping[p.srcTableIDNameMapping[effectiveRelID]].Exclude[columnName]
		return !isExcluded
	}

	addedColumnTypeOIDs := make([]uint32, 0)
	for _, column := range currRel.Columns {
		if isAddedColumnAndNotExcluded(column.Name) {
			addedColumnTypeOIDs = append(addedColumnTypeOIDs, column.DataType)
		}
	}

	typeSchemaNameMapping, err := p.GetSchemaNameOfColumnTypeByOID(ctx, addedColumnTypeOIDs)
	if err != nil {
		return nil, fmt.Errorf("error getting schema names for added column types: %w", err)
	}

	for _, column := range currRel.Columns {
		// not present in previous relation message, but in current one, so added.
		if isAddedColumnAndNotExcluded(column.Name) {
			schemaDelta.AddedColumns = append(schemaDelta.AddedColumns, &protos.FieldDescription{
				Name:           column.Name,
				Type:           currRelMap[column.Name],
				TypeModifier:   column.TypeModifier,
				Nullable:       false,
				TypeSchemaName: typeSchemaNameMapping[column.DataType],
			})
			// pg does not send nullable info, only whether column is part of replica identity
			// After loop we will correct this based on pg_catalog,
			// but can skip specific scenario where replident is default or index
			if currRel.ReplicaIdentity == uint8(ReplicaIdentityFull) ||
				currRel.ReplicaIdentity == uint8(ReplicaIdentityNothing) || column.Flags == 0 {
				potentiallyNullableAddedColumns = append(potentiallyNullableAddedColumns, utils.QuoteLiteral(column.Name))
			}
			p.logger.Info("Detected added column",
				slog.String("columnName", column.Name),
				slog.String("columnType", currRelMap[column.Name]),
				slog.String("relationName", schemaDelta.SrcTableName))
		} else if _, inPrevRel := prevRelMap[column.Name]; !inPrevRel {
			// Column is added but excluded
			p.logger.Warn(fmt.Sprintf("Detected added column %s in table %s, but not propagating because excluded",
				column.Name, schemaDelta.SrcTableName))
		} else if prevRelMap[column.Name] != currRelMap[column.Name] {
			p.logger.Warn(fmt.Sprintf("Detected column %s with type changed from %s to %s in table %s, but not propagating",
				column.Name, prevRelMap[column.Name], currRelMap[column.Name], schemaDelta.SrcTableName))
		}
	}
	for _, column := range prevSchema.Columns {
		// present in previous relation message, but not in current one, so dropped.
		if _, ok := currRelMap[column.Name]; !ok {
			p.logger.Warn(fmt.Sprintf("Detected dropped column %s in table %s, but not propagating", column,
				schemaDelta.SrcTableName))
		}
	}
	if len(potentiallyNullableAddedColumns) > 0 {
		p.logger.Info("Checking for potentially nullable columns in table",
			slog.String("tableName", schemaDelta.SrcTableName),
			slog.Any("potentiallyNullable", potentiallyNullableAddedColumns))

		rows, err := p.conn.Query(
			ctx,
			fmt.Sprintf(
				"select attname from pg_attribute where attrelid=$1 and attname in (%s) and not attnotnull",
				strings.Join(potentiallyNullableAddedColumns, ","),
			),
			currRel.RelationID,
		)
		if err != nil {
			return nil, fmt.Errorf("error looking up column nullable info for schema change: %w", err)
		}

		attnames, err := pgx.CollectRows[string](rows, pgx.RowTo)
		if err != nil {
			return nil, fmt.Errorf("error collecting rows for column nullable info for schema change: %w", err)
		}
		for _, column := range schemaDelta.AddedColumns {
			if slices.Contains(attnames, column.Name) {
				column.Nullable = true
				p.logger.Info(fmt.Sprintf("Detected column %s in table %s as nullable",
					column.Name, schemaDelta.SrcTableName))
			}
		}
	}

	p.relationMessageMapping[currRel.RelationID] = currRel
	// only log audit if there is actionable delta
	if len(schemaDelta.AddedColumns) > 0 {
		return &model.RelationRecord[Items]{
			BaseRecord:       p.baseRecord(lsn),
			TableSchemaDelta: schemaDelta,
		}, monitoring.AuditSchemaDelta(ctx, p.catalogPool.Pool, p.flowJobName, schemaDelta)
	}
	return nil, nil
}

// getParentRelIDIfPartitioned checks if the relation ID is a child table
// and returns the parent relation ID if it is.
// If the relation ID is not a child table, it returns the original relation ID.
// It also logs if the child table is detected for the first time.
func (p *PostgresCDCSource) getParentRelIDIfPartitioned(relID uint32) uint32 {
	parentRelID, ok := p.childToParentRelIDMapping[relID]
	if ok {
		if _, ok := p.hushWarnUnknownTableDetected[relID]; !ok {
			p.logger.Info("Detected child table in CDC stream, remapping to parent table",
				slog.Uint64("childRelID", uint64(relID)),
				slog.Uint64("parentRelID", uint64(parentRelID)),
				slog.String("parentTableName", p.srcTableIDNameMapping[parentRelID]))
			p.hushWarnUnknownTableDetected[relID] = struct{}{}
		}
		return parentRelID
	}

	return relID
}

// since we generate the childToParent mapping at the beginning of the CDC stream
// some child tables could be created after the CDC stream starts
// and we need to check if they inherit from a known table
// filtered by relkind; parent needs to be a partitioned table by default
func (p *PostgresCDCSource) checkIfUnknownTableInherits(ctx context.Context,
	relID uint32,
) (uint32, error) {
	relID = p.getParentRelIDIfPartitioned(relID)
	relkinds := "'p'"
	if p.handleInheritanceForNonPartitionedTables {
		relkinds = "'p', 'r'"
	}

	if _, ok := p.srcTableIDNameMapping[relID]; !ok {
		var parentRelID uint32
		if err := p.conn.QueryRow(
			ctx,
			fmt.Sprintf(`SELECT inhparent FROM pg_inherits
			JOIN pg_class c ON pg_inherits.inhparent=c.oid
			WHERE inhrelid=$1 AND c.relkind IN (%s)`, relkinds),
			relID,
		).Scan(&parentRelID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return relID, nil
			}
			return 0, fmt.Errorf("failed to query pg_inherits: %w", err)
		}
		p.childToParentRelIDMapping[relID] = parentRelID
		p.hushWarnUnknownTableDetected[relID] = struct{}{}
		p.logger.Info("Detected new child table in CDC stream, remapping to parent table",
			slog.Uint64("childRelID", uint64(relID)),
			slog.Uint64("parentRelID", uint64(parentRelID)),
			slog.String("parentTableName", p.srcTableIDNameMapping[parentRelID]))
		return parentRelID, nil
	}

	return relID, nil
}
