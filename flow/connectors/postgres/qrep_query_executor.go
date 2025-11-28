package connpostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	jsoniter "github.com/json-iterator/go"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type QRepQueryExecutor struct {
	*PostgresConnector
	logger      log.Logger
	env         map[string]string
	snapshot    string
	flowJobName string
	partitionID string
	version     uint32
}

func (c *PostgresConnector) NewQRepQueryExecutor(ctx context.Context, env map[string]string, version uint32,
	flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	return c.NewQRepQueryExecutorSnapshot(ctx, env, version, "", flowJobName, partitionID)
}

func (c *PostgresConnector) NewQRepQueryExecutorSnapshot(ctx context.Context, env map[string]string, version uint32,
	snapshot string, flowJobName string, partitionID string,
) (*QRepQueryExecutor, error) {
	if _, err := c.fetchCustomTypeMapping(ctx); err != nil {
		c.logger.Error("[pg_query_executor] failed to fetch custom type mapping", slog.Any("error", err))
		return nil, fmt.Errorf("failed to fetch custom type mapping: %w", err)
	}
	return &QRepQueryExecutor{
		PostgresConnector: c,
		env:               env,
		snapshot:          snapshot,
		flowJobName:       flowJobName,
		partitionID:       partitionID,
		logger:            log.With(c.logger, slog.String(string(shared.PartitionIDKey), partitionID)),
		version:           version,
	}, nil
}

func (qe *QRepQueryExecutor) ExecuteQuery(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	rows, err := qe.conn.Query(ctx, query, args...)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to execute query", slog.Any("error", err))
		return nil, err
	}
	return rows, nil
}

// FieldDescriptionsToSchema converts a slice of pgconn.FieldDescription to a QRecordSchema.
func (qe *QRepQueryExecutor) cursorToSchema(
	ctx context.Context,
	tx pgx.Tx,
	cursorName string,
) (types.QRecordSchema, *types.NullableSchemaDebug, error) {
	laxMode, err := internal.PeerDBNullableLax(ctx, qe.env)
	if err != nil {
		return types.QRecordSchema{}, nil, err
	}

	rows, err := tx.Query(ctx, "FETCH 0 FROM "+cursorName)
	if err != nil {
		return types.QRecordSchema{}, nil, fmt.Errorf("failed to fetch 0 for field descriptions: %w", err)
	}
	fds := rows.FieldDescriptions()
	rows.Close()

	tableOIDset := make(map[uint32]struct{})
	qfields := make([]types.QField, len(fds))

	// In lax mode: track debug info and map attIds to field indices
	// In strict mode: track pointers to nullable fields
	var schemaDebug *types.NullableSchemaDebug
	var attIdToFieldIdx map[attId][]int // lax mode
	var nullPointers map[attId]*bool    // strict mode

	if laxMode {
		schemaDebug = &types.NullableSchemaDebug{
			PgxFields:      make([]types.PgxFieldDebug, len(fds)),
			StrictNullable: make([]bool, len(fds)),
		}
		attIdToFieldIdx = make(map[attId][]int, len(fds))
	} else {
		nullPointers = make(map[attId]*bool, len(fds))
	}

	for i, fd := range fds {
		tableOIDset[fd.TableOID] = struct{}{}
		ctype := qe.postgresOIDToQValueKind(fd.DataTypeOID, qe.customTypeMapping, qe.version)

		if ctype == types.QValueKindNumeric || ctype == types.QValueKindArrayNumeric {
			precision, scale := datatypes.ParseNumericTypmod(fd.TypeModifier)
			qfields[i] = types.QField{
				Name:      fd.Name,
				Type:      ctype,
				Nullable:  laxMode, // lax=true, strict=false (until pg_attribute says otherwise)
				Precision: precision,
				Scale:     scale,
			}
		} else {
			qfields[i] = types.QField{
				Name:     fd.Name,
				Type:     ctype,
				Nullable: laxMode,
			}
		}

		key := attId{relid: fd.TableOID, num: fd.TableAttributeNumber}
		if laxMode {
			schemaDebug.PgxFields[i] = types.PgxFieldDebug{
				Name:                 fd.Name,
				TableOID:             fd.TableOID,
				TableAttributeNumber: fd.TableAttributeNumber,
				DataTypeOID:          fd.DataTypeOID,
			}
			attIdToFieldIdx[key] = append(attIdToFieldIdx[key], i)
		} else {
			nullPointers[key] = &qfields[i].Nullable
		}
	}

	tableOIDs := slices.Collect(maps.Keys(tableOIDset))
	if laxMode {
		schemaDebug.QueriedTableOIDs = tableOIDs
	}

	// Query pg_attribute - different queries for lax vs strict
	if laxMode {
		if err := qe.populateLaxModeDebugInfo(ctx, tx, tableOIDs, schemaDebug, attIdToFieldIdx); err != nil {
			return types.QRecordSchema{}, nil, err
		}
	} else {
		// Strict mode: minimal query, just need nullable columns
		rows, err := tx.Query(ctx,
			"SELECT a.attrelid, a.attnum FROM pg_attribute a WHERE a.attrelid = ANY($1) AND NOT a.attnotnull",
			tableOIDs)
		if err != nil {
			return types.QRecordSchema{}, nil, fmt.Errorf("failed to query pg_attribute: %w", err)
		}

		var att attId
		if _, err := pgx.ForEachRow(rows, []any{&att.relid, &att.num}, func() error {
			if nullPointer, ok := nullPointers[att]; ok {
				*nullPointer = true
			}
			return nil
		}); err != nil {
			return types.QRecordSchema{}, nil, fmt.Errorf("failed to process pg_attribute: %w", err)
		}
	}

	return types.NewQRecordSchema(qfields), schemaDebug, nil
}

type attId struct {
	relid uint32
	num   uint16
}

// populateLaxModeDebugInfo populates debug info for diagnosing nullable mismatches.
// The aim is to capture enough data that the customer can change the schema in any way
// after the snapshot transaction is done and we still have a way to debug.
func (qe *QRepQueryExecutor) populateLaxModeDebugInfo(
	ctx context.Context,
	tx pgx.Tx,
	tableOIDs []uint32,
	schemaDebug *types.NullableSchemaDebug,
	attIdToFieldIdx map[attId][]int,
) error {
	// First, expand tableOIDs to include all parent tables (for full column info)
	allTableOIDs := make(map[uint32]struct{})
	for _, oid := range tableOIDs {
		allTableOIDs[oid] = struct{}{}
	}
	parentOIDByTableOID := make(map[uint32]uint32)

	// Iteratively find all parent tables
	oidsToQuery := tableOIDs
	for len(oidsToQuery) > 0 {
		rows, err := tx.Query(ctx, `SELECT inhrelid, inhparent FROM pg_inherits WHERE inhrelid = ANY($1)`, oidsToQuery)
		if err != nil {
			return fmt.Errorf("failed to query pg_inherits: %w", err)
		}
		var childOID, parentOID uint32
		var nextOids []uint32
		if _, err := pgx.ForEachRow(rows, []any{&childOID, &parentOID}, func() error {
			parentOIDByTableOID[childOID] = parentOID
			if _, seen := allTableOIDs[parentOID]; !seen {
				allTableOIDs[parentOID] = struct{}{}
				nextOids = append(nextOids, parentOID)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed to process pg_inherits: %w", err)
		}
		oidsToQuery = nextOids
	}

	allOIDSlice := slices.Collect(maps.Keys(allTableOIDs))

	// Query pg_attribute for ALL tables (children + parents)
	rows, err := tx.Query(ctx, `
		SELECT a.attrelid, a.attnum, a.attname, a.attnotnull, a.atttypid, a.attinhcount, a.attislocal
		FROM pg_attribute a
		WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attrelid, a.attnum`,
		allOIDSlice)
	if err != nil {
		return fmt.Errorf("failed to query pg_attribute: %w", err)
	}

	var row types.PgAttributeDebug
	if _, err := pgx.ForEachRow(rows, []any{
		&row.AttRelID, &row.AttNum, &row.AttName, &row.AttNotNull, &row.AttTypID, &row.AttInhCount, &row.AttIsLocal,
	}, func() error {
		schemaDebug.PgAttributeRows = append(schemaDebug.PgAttributeRows, row)

		// Compute strict nullable: if NOT attnotnull and matches a field, mark it nullable
		if !row.AttNotNull {
			key := attId{relid: row.AttRelID, num: uint16(row.AttNum)}
			if indices, ok := attIdToFieldIdx[key]; ok {
				for _, idx := range indices {
					schemaDebug.StrictNullable[idx] = true
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to process pg_attribute: %w", err)
	}

	// Query table names and schemas for all tables
	rows, err = tx.Query(ctx, `
		SELECT c.oid, c.relname, n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.oid = ANY($1)`,
		allOIDSlice)
	if err != nil {
		return fmt.Errorf("failed to query pg_class: %w", err)
	}

	var oid uint32
	var tableName, schemaName string
	if _, err := pgx.ForEachRow(rows, []any{&oid, &tableName, &schemaName}, func() error {
		schemaDebug.Tables = append(schemaDebug.Tables, types.TableDebug{
			OID:        oid,
			TableName:  tableName,
			SchemaName: schemaName,
			ParentOID:  parentOIDByTableOID[oid],
		})
		return nil
	}); err != nil {
		return fmt.Errorf("failed to process pg_class: %w", err)
	}

	return nil
}

func (qe *QRepQueryExecutor) processRowsStream(
	ctx context.Context,
	cursorName string,
	dstType protos.DBType,
	stream *model.QRecordStream,
	rows pgx.Rows,
	fieldDescriptions []pgconn.FieldDescription,
) (int64, int64, error) {
	var numRows int64
	var numBytes int64
	const logPerRows = 50000

	jsonApi := createExtendedJSONUnmarshaler()
	schema, err := stream.Schema()
	if err != nil {
		return 0, 0, err
	}
	nullableFields := make(map[string]struct{}, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.Nullable {
			nullableFields[field.Name] = struct{}{}
		}
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			qe.logger.Info("Context canceled, exiting processRowsStream early")
			return numRows, numBytes, err
		}

		record, err := qe.mapRowToQRecord(rows, dstType, nullableFields, fieldDescriptions, jsonApi)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to map row to QRecord", slog.Any("error", err))
			return numRows, numBytes, fmt.Errorf("failed to map row to QRecord: %w", err)
		}
		stream.Records <- record
		numRows++
		for _, val := range rows.RawValues() {
			numBytes += int64(len(val))
		}

		if numRows%logPerRows == 0 {
			qe.logger.Info("processing row stream",
				slog.String("cursor", cursorName),
				slog.Int64("records", numRows),
				slog.Int64("bytes", numBytes),
				slog.Int("channelLen", len(stream.Records)))
		}
	}

	qe.logger.Info("processed row stream",
		slog.String("cursor", cursorName),
		slog.Int64("records", numRows),
		slog.Int64("bytes", numBytes),
		slog.Int("channelLen", len(stream.Records)))
	return numRows, numBytes, nil
}

func (qe *QRepQueryExecutor) processFetchedRows(
	ctx context.Context,
	query string,
	tx pgx.Tx,
	cursorName string,
	fetchSize int,
	dstType protos.DBType,
	stream *model.QRecordStream,
) (int64, int64, error) {
	qe.logger.Info("[pg_query_executor] fetching from cursor", slog.String("cursor", cursorName))

	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", fetchSize, cursorName))
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to fetch cursor in tx",
			slog.Any("error", err), slog.String("query", query))
		return 0, 0, fmt.Errorf("[pg_query_executor] failed to execute query in tx: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	numRows, numBytes, err := qe.processRowsStream(ctx, cursorName, dstType, stream, rows, fieldDescriptions)
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to process rows", slog.Any("error", err))
		return numRows, numBytes, fmt.Errorf("failed to process rows: %w", err)
	}

	if err := rows.Err(); err != nil {
		qe.logger.Error("[pg_query_executor] row iteration failed",
			slog.String("query", query), slog.Any("error", err))
		return numRows, numBytes, fmt.Errorf("[pg_query_executor] row iteration failed '%s': %w", query, err)
	}

	return numRows, numBytes, nil
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQuery(
	ctx context.Context,
	query string,
	args ...any,
) (*model.QRecordBatch, error) {
	stream := model.NewQRecordStream(1024)
	errors := make(chan struct{})
	var errorsError error
	qe.logger.Info("Executing and processing query", slog.String("query", query))

	// must wait on errors to close before returning to maintain qe.conn exclusion
	go func() {
		defer close(errors)
		if _, _, err := qe.ExecuteAndProcessQueryStream(ctx, stream, protos.DBType_DBTYPE_UNKNOWN, query, args...); err != nil {
			qe.logger.Error("[pg_query_executor] failed to execute and process query stream", slog.Any("error", err))
			errorsError = err
		}
	}()

	select {
	case <-errors:
		if errorsError == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-stream.SchemaChan():
				schema, err := stream.Schema()
				if err != nil {
					return nil, err
				}
				return &model.QRecordBatch{
					Schema:  schema,
					Records: nil,
				}, nil
			}
		}
		return nil, errorsError
	case <-stream.SchemaChan():
		schema, err := stream.Schema()
		if err != nil {
			return nil, err
		}
		batch := &model.QRecordBatch{
			Schema:  schema,
			Records: nil,
		}
		for record := range stream.Records {
			batch.Records = append(batch.Records, record)
		}
		<-errors
		if errorsError != nil {
			return nil, errorsError
		}
		if err := stream.Err(); err != nil {
			return nil, fmt.Errorf("[pg] failed to get record from stream: %w", err)
		}
		return batch, nil
	}
}

func (qe *QRepQueryExecutor) ExecuteAndProcessQueryStream(
	ctx context.Context,
	stream *model.QRecordStream,
	dstType protos.DBType,
	query string,
	args ...any,
) (int64, int64, error) {
	return qe.ExecuteQueryIntoSink(
		ctx,
		RecordStreamSink{QRecordStream: stream, DestinationType: dstType},
		query,
		args...,
	)
}

func (qe *QRepQueryExecutor) ExecuteQueryIntoSink(
	ctx context.Context,
	sink QRepPullSink,
	query string,
	args ...any,
) (int64, int64, error) {
	qe.logger.Info("Executing and processing query stream", slog.String("query", query))
	defer sink.Close(nil)

	tx, err := qe.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to begin transaction", slog.Any("error", err))
		err := fmt.Errorf("[pg_query_executor] failed to begin transaction: %w", err)
		sink.Close(err)
		return 0, 0, err
	}

	totalRecords, totalBytes, err := sink.ExecuteQueryWithTx(ctx, qe, tx, query, args...)
	if err != nil {
		sink.Close(err)
	}
	return totalRecords, totalBytes, err
}

func (qe *QRepQueryExecutor) ExecuteQueryIntoSinkGettingCurrentSnapshotXmin(
	ctx context.Context,
	sink QRepPullSink,
	query string,
	args ...any,
) (int64, int64, int64, error) {
	var currentSnapshotXmin pgtype.Int8
	qe.logger.Info("Executing and processing query stream", slog.String("query", query))
	defer sink.Close(nil)

	tx, err := qe.conn.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		qe.logger.Error("[pg_query_executor] failed to begin transaction", slog.Any("error", err))
		err := fmt.Errorf("[pg_query_executor] failed to begin transaction: %w", err)
		sink.Close(err)
		return 0, 0, currentSnapshotXmin.Int64, err
	}

	if err := tx.QueryRow(ctx, "select txid_snapshot_xmin(txid_current_snapshot())").Scan(&currentSnapshotXmin); err != nil {
		qe.logger.Error("[pg_query_executor] failed to get current snapshot xmin", slog.Any("error", err))
		sink.Close(err)
		return 0, 0, currentSnapshotXmin.Int64, err
	}

	totalRecords, totalBytes, err := sink.ExecuteQueryWithTx(ctx, qe, tx, query, args...)
	if err != nil {
		sink.Close(err)
	}
	return totalRecords, totalBytes, currentSnapshotXmin.Int64, err
}

func (qe *QRepQueryExecutor) mapRowToQRecord(
	row pgx.Rows,
	dstType protos.DBType,
	nullableFields map[string]struct{},
	fds []pgconn.FieldDescription,
	jsonApi jsoniter.API,
) ([]types.QValue, error) {
	rawValues := row.RawValues()
	values := make([]any, len(fds))
	// Simulate the behavior of rows.Values() with a carveout for JSON
	for i, fd := range fds {
		buf := rawValues[i]
		if buf == nil {
			values[i] = nil
			continue
		}

		// Special handling for JSON types
		switch fd.DataTypeOID {
		case pgtype.JSONOID, pgtype.JSONBOID:
			if err := jsonApi.Unmarshal(buf, &values[i]); err != nil {
				qe.logger.Error("[pg_query_executor] failed to unmarshal json", slog.Any("error", err))
				return nil, fmt.Errorf("failed to unmarshal json: %w", err)
			}
			if values[i] == nil {
				// avoid confusing SQL null & JSON null by using pre-marshaled value
				values[i] = json.RawMessage("null")
			}
		case pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
			var textArr pgtype.FlatArray[pgtype.Text]
			if err := qe.conn.TypeMap().Scan(fd.DataTypeOID, fd.Format, buf, &textArr); err != nil {
				qe.logger.Error("[pg_query_executor] failed to to scan json array", slog.Any("error", err))
				return nil, fmt.Errorf("failed to scan json array: %w", err)
			}

			arr := make([]any, len(textArr))
			for j, text := range textArr {
				if text.Valid {
					if err := jsonApi.UnmarshalFromString(text.String, &arr[j]); err != nil {
						qe.logger.Error("[pg_query_executor] failed to unmarshal json array element", slog.Any("error", err))
						return nil, fmt.Errorf("failed to unmarshal json array element: %w", err)
					}
				} else {
					arr[j] = nil
				}
			}
			values[i] = arr

		default:
			if dt, ok := qe.conn.TypeMap().TypeForOID(fd.DataTypeOID); ok {
				value, err := dt.Codec.DecodeValue(qe.conn.TypeMap(), fd.DataTypeOID, fd.Format, buf)
				if err != nil {
					qe.logger.Error("[pg_query_executor] failed to decode value", slog.Any("error", err))
					return nil, fmt.Errorf("failed to decode value: %w", err)
				}
				values[i] = value
			} else {
				// Unknown type - treat as text or binary based on format
				switch fd.Format {
				case pgtype.TextFormatCode:
					values[i] = string(buf)
				case pgtype.BinaryFormatCode:
					values[i] = slices.Clone(buf)
				default:
					qe.logger.Error("[pg_query_executor] unknown format code", slog.Int("format", int(fd.Format)))
					return nil, fmt.Errorf("unknown format code: %d", fd.Format)
				}
			}
		}
	}

	// Schema fields should generally align with field descriptors,
	// avoid building map until we detect they are misaligned
	record := make([]types.QValue, len(fds))
	for i, fd := range fds {
		_, nullable := nullableFields[fd.Name]
		tmp, err := qe.parseFieldFromPostgresOID(
			fd.DataTypeOID,
			fd.TypeModifier,
			nullable,
			dstType,
			values[i],
			qe.customTypeMapping,
			qe.version,
		)
		if err != nil {
			qe.logger.Error("[pg_query_executor] failed to parse field", slog.Any("error", err))
			return nil, fmt.Errorf("failed to parse field: %w", err)
		}
		record[i] = tmp
	}

	return record, nil
}
