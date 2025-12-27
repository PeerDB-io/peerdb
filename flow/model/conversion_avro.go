package model

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"

	"github.com/hamba/avro/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

//nolint:govet // field alignment not worth readability cost
type QRecordAvroConverter struct {
	logger                   log.Logger
	Schema                   *QRecordAvroSchemaDefinition
	ColNames                 []string
	TargetDWH                protos.DBType
	UnboundedNumericAsString bool
	NullMismatchTracker      *NullMismatchTracker
}

func NewQRecordAvroConverter(
	ctx context.Context,
	env map[string]string,
	schema *QRecordAvroSchemaDefinition,
	targetDWH protos.DBType,
	colNames []string,
	logger log.Logger,
) (*QRecordAvroConverter, error) {
	var unboundedNumericAsString bool
	if targetDWH == protos.DBType_CLICKHOUSE {
		var err error
		unboundedNumericAsString, err = internal.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return nil, err
		}
	}

	return &QRecordAvroConverter{
		Schema:                   schema,
		TargetDWH:                targetDWH,
		ColNames:                 colNames,
		logger:                   logger,
		UnboundedNumericAsString: unboundedNumericAsString,
	}, nil
}

func (qac *QRecordAvroConverter) Convert(
	ctx context.Context,
	env map[string]string,
	qrecord []types.QValue,
	typeConversions map[string]types.TypeConversion,
	numericTruncator SnapshotTableNumericTruncator,
	format internal.BinaryFormat,
	calcSize bool,
) (map[string]any, int64, error) {
	m := make(map[string]any, len(qrecord))
	s := int64(0)
	for idx, val := range qrecord {
		if typeConversion, ok := typeConversions[qac.Schema.Fields[idx].Name]; ok {
			val = typeConversion.ValueConversion(val)
		}

		// Record the cases where the value is null AND strict mode would say not nullable
		if val.Value() == nil && qac.NullMismatchTracker != nil {
			qac.NullMismatchTracker.RecordNull(idx)
		}

		avroVal, size, err := qvalue.QValueToAvro(
			ctx, val,
			&qac.Schema.Fields[idx], qac.TargetDWH, qac.logger, qac.UnboundedNumericAsString,
			numericTruncator.Get(idx),
			format,
			calcSize,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to convert QValue to Avro-compatible value: %w", err)
		}

		m[qac.ColNames[idx]] = avroVal
		if calcSize {
			s += size
		}
	}

	return m, s, nil
}

type QRecordAvroField struct {
	Type any    `json:"type"`
	Name string `json:"name"`
}

type QRecordAvroSchema struct {
	Type   string             `json:"type"`
	Name   string             `json:"name"`
	Fields []QRecordAvroField `json:"fields"`
}

type QRecordAvroSchemaDefinition struct {
	Schema *avro.RecordSchema
	Fields []types.QField
}

type QRecordAvroChunkSizeTracker struct {
	TrackUncompressed bool
	Bytes             atomic.Int64
}

func GetAvroSchemaDefinition(
	ctx context.Context,
	env map[string]string,
	dstTableName string,
	qRecordSchema types.QRecordSchema,
	targetDWH protos.DBType,
	avroNameMap map[string]string,
) (*QRecordAvroSchemaDefinition, error) {
	avroFields := make([]*avro.Field, 0, len(qRecordSchema.Fields))

	for _, qField := range qRecordSchema.Fields {
		avroType, err := qvalue.GetAvroSchemaFromQValueKind(ctx, env, qField.Type, targetDWH, qField.Precision, qField.Scale)
		if err != nil {
			return nil, err
		}

		if qField.Nullable {
			avroType, err = qvalue.NullableAvroSchema(avroType)
			if err != nil {
				return nil, err
			}
		}

		avroFieldName := qField.Name
		if avroNameMap != nil {
			avroFieldName = avroNameMap[qField.Name]
		}

		avroField, err := avro.NewField(avroFieldName, avroType)
		if err != nil {
			return nil, err
		}
		avroFields = append(avroFields, avroField)
	}

	if targetDWH == protos.DBType_CLICKHOUSE {
		dstTableName = qvalue.ConvertToAvroCompatibleName(dstTableName)
	}
	avroSchema, err := avro.NewRecordSchema(dstTableName, "", avroFields)
	if err != nil {
		return nil, err
	}

	return &QRecordAvroSchemaDefinition{
		Schema: avroSchema,
		Fields: qRecordSchema.Fields,
	}, nil
}

func ConstructColumnNameAvroFieldMap(fields []types.QField) map[string]string {
	m := make(map[string]string, len(fields))
	for i, field := range fields {
		m[field.Name] = qvalue.ConvertToAvroCompatibleName(field.Name) + "_" + strconv.FormatInt(int64(i), 10)
	}
	return m
}

// NullMismatchTracker detects null values in columns that would be non-nullable under strict mode
type NullMismatchTracker struct {
	schemaDebug    *types.NullableSchemaDebug
	mismatchedCols []bool
}

func NewNullMismatchTracker(
	schemaDebug *types.NullableSchemaDebug,
) *NullMismatchTracker {
	if schemaDebug == nil {
		return nil // Not in lax mode
	}
	return &NullMismatchTracker{
		schemaDebug:    schemaDebug,
		mismatchedCols: make([]bool, len(schemaDebug.StrictNullable)),
	}
}

func (t *NullMismatchTracker) RecordNull(fieldIdx int) {
	if fieldIdx < len(t.schemaDebug.StrictNullable) && !t.schemaDebug.StrictNullable[fieldIdx] {
		t.mismatchedCols[fieldIdx] = true
	}
}

func (t *NullMismatchTracker) LogIfMismatch(ctx context.Context, logger log.Logger) {
	// Categorize mismatched columns by the reason they were marked non-nullable
	var lookupFailedCols []string // pg_attribute lookup failed (table OID or attnum mismatch)
	var notNullInPgCols []string  // Found in pg_attribute but marked NOT NULL

	for idx, mismatched := range t.mismatchedCols {
		if !mismatched || idx >= len(t.schemaDebug.PgxFields) {
			continue
		}

		colName := t.schemaDebug.PgxFields[idx].Name
		if idx < len(t.schemaDebug.MatchFound) && !t.schemaDebug.MatchFound[idx] {
			lookupFailedCols = append(lookupFailedCols, colName)
		} else {
			notNullInPgCols = append(notNullInPgCols, colName)
		}
	}

	if len(lookupFailedCols) == 0 && len(notNullInPgCols) == 0 {
		return
	}

	// Dump the ENTIRE schema debug info - all pgx fields, pg_attribute rows, and table metadata
	logger.Warn("Null values in columns that would be non-nullable under strict mode",
		slog.Any("lookup_failed_columns", lookupFailedCols),
		slog.Any("not_null_in_pg_attribute_columns", notNullInPgCols),
		slog.Any("queried_table_oids", t.schemaDebug.QueriedTableOIDs),
		slog.Any("pgx_field_descriptions", t.schemaDebug.PgxFields),
		slog.Any("pg_attribute_rows", t.schemaDebug.PgAttributeRows),
		slog.Any("tables_with_inheritance", t.schemaDebug.Tables),
	)

	otel_metrics.CodeNotificationCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("message", "Null values in columns that would be non-nullable under strict mode"),
	)))
}
