package peersql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type SQLQueryExecutor interface {
	ConnectionActive(context.Context) error
	Close() error

	CreateSchema(ctx context.Context, schemaName string) error
	DropSchema(ctx context.Context, schemaName string) error
	CheckSchemaExists(ctx context.Context, schemaName string) (bool, error)
	RecreateSchema(ctx context.Context, schemaName string) error

	CreateTable(ctx context.Context, schema *qvalue.QRecordSchema, schemaName string, tableName string) error
	CountRows(ctx context.Context, schemaName string, tableName string) (int64, error)

	ExecuteAndProcessQuery(ctx context.Context, query string, args ...interface{}) (*model.QRecordBatch, error)
	NamedExecuteAndProcessQuery(ctx context.Context, query string, arg interface{}) (*model.QRecordBatch, error)
	ExecuteQuery(ctx context.Context, query string, args ...interface{}) error
	NamedExec(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

type GenericSQLQueryExecutor struct {
	db                 *sqlx.DB
	dbtypeToQValueKind map[string]qvalue.QValueKind
	qvalueKindToDBType map[qvalue.QValueKind]string
	logger             log.Logger
}

func NewGenericSQLQueryExecutor(
	logger log.Logger,
	db *sqlx.DB,
	dbtypeToQValueKind map[string]qvalue.QValueKind,
	qvalueKindToDBType map[qvalue.QValueKind]string,
) *GenericSQLQueryExecutor {
	return &GenericSQLQueryExecutor{
		db:                 db,
		dbtypeToQValueKind: dbtypeToQValueKind,
		qvalueKindToDBType: qvalueKindToDBType,
		logger:             logger,
	}
}

func (g *GenericSQLQueryExecutor) ConnectionActive(ctx context.Context) bool {
	err := g.db.PingContext(ctx)
	return err == nil
}

func (g *GenericSQLQueryExecutor) Close() error {
	return g.db.Close()
}

func (g *GenericSQLQueryExecutor) CreateSchema(ctx context.Context, schemaName string) error {
	_, err := g.db.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	return err
}

func (g *GenericSQLQueryExecutor) DropSchema(ctx context.Context, schemaName string) error {
	_, err := g.db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
	return err
}

// the SQL query this function executes appears to be MySQL/MariaDB specific.
func (g *GenericSQLQueryExecutor) CheckSchemaExists(ctx context.Context, schemaName string) (bool, error) {
	var exists pgtype.Bool
	// use information schemata to check if schema exists
	err := g.db.QueryRowxContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", schemaName).Scan(&exists)
	return exists.Bool, err
}

func (g *GenericSQLQueryExecutor) RecreateSchema(ctx context.Context, schemaName string) error {
	err := g.DropSchema(ctx, schemaName)
	if err != nil {
		return fmt.Errorf("failed to drop schema: %w", err)
	}

	err = g.CreateSchema(ctx, schemaName)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	return nil
}

func (g *GenericSQLQueryExecutor) CreateTable(ctx context.Context, schema *qvalue.QRecordSchema, schemaName string, tableName string) error {
	fields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		dbType, ok := g.qvalueKindToDBType[field.Type]
		if !ok {
			return fmt.Errorf("unsupported qvalue type %s", field.Type)
		}
		fields = append(fields, fmt.Sprintf(`"%s" %s`, field.Name, dbType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(fields, ", "))

	_, err := g.db.ExecContext(ctx, command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (g *GenericSQLQueryExecutor) CountRows(ctx context.Context, schemaName string, tableName string) (int64, error) {
	var count pgtype.Int8
	err := g.db.QueryRowxContext(ctx, "SELECT COUNT(*) FROM "+schemaName+"."+tableName).Scan(&count)
	return count.Int64, err
}

func (g *GenericSQLQueryExecutor) CountNonNullRows(
	ctx context.Context,
	schemaName string,
	tableName string,
	columnName string,
) (int64, error) {
	var count pgtype.Int8
	err := g.db.QueryRowxContext(ctx, "SELECT COUNT(CASE WHEN "+columnName+
		" IS NOT NULL THEN 1 END) AS non_null_count FROM "+schemaName+"."+tableName).Scan(&count)
	return count.Int64, err
}

func (g *GenericSQLQueryExecutor) CountSRIDs(
	ctx context.Context,
	schemaName string,
	tableName string,
	columnName string,
) (int64, error) {
	var count pgtype.Int8
	err := g.db.QueryRowxContext(ctx, "SELECT COUNT(CASE WHEN ST_SRID("+columnName+
		") <> 0 THEN 1 END) AS not_zero FROM "+schemaName+"."+tableName).Scan(&count)
	return count.Int64, err
}

func (g *GenericSQLQueryExecutor) columnTypeToQField(ct *sql.ColumnType) (qvalue.QField, error) {
	qvKind, ok := g.dbtypeToQValueKind[ct.DatabaseTypeName()]
	if !ok {
		return qvalue.QField{}, fmt.Errorf("unsupported database type %s", ct.DatabaseTypeName())
	}

	nullable, ok := ct.Nullable()

	return qvalue.QField{
		Name:     ct.Name(),
		Type:     qvKind,
		Nullable: ok && nullable,
	}, nil
}

func (g *GenericSQLQueryExecutor) processRows(rows *sqlx.Rows) (*model.QRecordBatch, error) {
	dbColTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert dbColTypes to QFields
	qfields := make([]qvalue.QField, len(dbColTypes))
	for i, ct := range dbColTypes {
		qfield, err := g.columnTypeToQField(ct)
		if err != nil {
			g.logger.Error(fmt.Sprintf("failed to convert column type %v", ct),
				slog.Any("error", err))
			return nil, err
		}
		qfields[i] = qfield
	}

	var records [][]qvalue.QValue
	totalRowsProcessed := 0
	const logEveryNumRows = 50000

	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(columns))
		for i := range values {
			switch qfields[i].Type {
			case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindTime,
				qvalue.QValueKindTimeTZ, qvalue.QValueKindDate:
				var t sql.NullTime
				values[i] = &t
			case qvalue.QValueKindInt16:
				var n sql.NullInt16
				values[i] = &n
			case qvalue.QValueKindInt32:
				var n sql.NullInt32
				values[i] = &n
			case qvalue.QValueKindInt64:
				var n sql.NullInt64
				values[i] = &n
			case qvalue.QValueKindFloat32:
				var f sql.NullFloat64
				values[i] = &f
			case qvalue.QValueKindFloat64:
				var f sql.NullFloat64
				values[i] = &f
			case qvalue.QValueKindBoolean:
				var b sql.NullBool
				values[i] = &b
			case qvalue.QValueKindString, qvalue.QValueKindHStore:
				var s sql.NullString
				values[i] = &s
			case qvalue.QValueKindBytes:
				values[i] = new([]byte)
			case qvalue.QValueKindNumeric:
				var s sql.Null[decimal.Decimal]
				values[i] = &s
			case qvalue.QValueKindUUID:
				values[i] = new([]byte)
			default:
				values[i] = new(interface{})
			}
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		qValues := make([]qvalue.QValue, len(values))
		for i, val := range values {
			qv, err := toQValue(qfields[i].Type, val)
			if err != nil {
				g.logger.Error("failed to convert value", slog.Any("error", err))
				return nil, err
			}
			qValues[i] = qv
		}

		records = append(records, qValues)
		totalRowsProcessed += 1

		if totalRowsProcessed%logEveryNumRows == 0 {
			g.logger.Info("processed rows", slog.Int("rows", totalRowsProcessed))
		}
	}

	if err := rows.Err(); err != nil {
		g.logger.Error("failed to iterate over rows", slog.Any("Error", err))
		return nil, err
	}

	return &model.QRecordBatch{
		Schema:  qvalue.NewQRecordSchema(qfields),
		Records: records,
	}, nil
}

func (g *GenericSQLQueryExecutor) ExecuteAndProcessQuery(
	ctx context.Context,
	query string,
	args ...interface{},
) (*model.QRecordBatch, error) {
	rows, err := g.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return g.processRows(rows)
}

func (g *GenericSQLQueryExecutor) NamedExecuteAndProcessQuery(
	ctx context.Context,
	query string,
	arg interface{},
) (*model.QRecordBatch, error) {
	rows, err := g.db.NamedQueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return g.processRows(rows)
}

func (g *GenericSQLQueryExecutor) ExecuteQuery(ctx context.Context, query string, args ...interface{}) error {
	_, err := g.db.ExecContext(ctx, query, args...)
	return err
}

func (g *GenericSQLQueryExecutor) NamedExec(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return g.db.NamedExecContext(ctx, query, arg)
}

// returns true if any of the columns are null in value
func (g *GenericSQLQueryExecutor) CheckNull(ctx context.Context, schema string, tableName string, colNames []string) (bool, error) {
	var count pgtype.Int8
	joinedString := strings.Join(colNames, " is null or ") + " is null"
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s",
		schema, tableName, joinedString)

	err := g.db.QueryRowxContext(ctx, query).Scan(&count)
	if err != nil {
		return false, err
	}

	return count.Int64 == 0, nil
}

func toQValue(kind qvalue.QValueKind, val interface{}) (qvalue.QValue, error) {
	if val == nil {
		return qvalue.QValueNull(kind), nil
	}
	switch kind {
	case qvalue.QValueKindInt32:
		if v, ok := val.(*sql.NullInt32); ok {
			if v.Valid {
				return qvalue.QValueInt32{Val: v.Int32}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindInt32), nil
			}
		}
	case qvalue.QValueKindInt64:
		if v, ok := val.(*sql.NullInt64); ok {
			if v.Valid {
				return qvalue.QValueInt64{Val: v.Int64}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindInt64), nil
			}
		}
	case qvalue.QValueKindFloat32:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return qvalue.QValueFloat32{Val: float32(v.Float64)}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindFloat32), nil
			}
		}
	case qvalue.QValueKindFloat64:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return qvalue.QValueFloat64{Val: v.Float64}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindFloat64), nil
			}
		}
	case qvalue.QValueKindQChar:
		if v, ok := val.(uint8); ok {
			return qvalue.QValueQChar{Val: v}, nil
		}
	case qvalue.QValueKindString:
		if v, ok := val.(*sql.NullString); ok {
			if v.Valid {
				return qvalue.QValueString{Val: v.String}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindString), nil
			}
		}
	case qvalue.QValueKindBoolean:
		if v, ok := val.(*sql.NullBool); ok {
			if v.Valid {
				return qvalue.QValueBoolean{Val: v.Bool}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindBoolean), nil
			}
		}
	case qvalue.QValueKindTimestamp:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return qvalue.QValueTimestamp{Val: t.Time}, nil
			} else {
				return qvalue.QValueNull(kind), nil
			}
		}
	case qvalue.QValueKindTimestampTZ:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return qvalue.QValueTimestampTZ{Val: t.Time}, nil
			} else {
				return qvalue.QValueNull(kind), nil
			}
		}
	case qvalue.QValueKindDate:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return qvalue.QValueDate{Val: t.Time}, nil
			} else {
				return qvalue.QValueNull(kind), nil
			}
		}
	case qvalue.QValueKindTime:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				tt := t.Time
				// anchor on unix epoch, some drivers anchor on 0001-01-01
				return qvalue.QValueTimeTZ{
					Val: time.Date(1970, time.January, 1, tt.Hour(), tt.Minute(), tt.Second(), tt.Nanosecond(), time.UTC),
				}, nil
			} else {
				return qvalue.QValueNull(kind), nil
			}
		}
	case qvalue.QValueKindTimeTZ:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				tt := t.Time
				// anchor on unix epoch, some drivers anchor on 0001-01-01
				return qvalue.QValueTimeTZ{
					Val: time.Date(1970, time.January, 1, tt.Hour(), tt.Minute(), tt.Second(), tt.Nanosecond(), tt.Location()),
				}, nil
			} else {
				return qvalue.QValueNull(kind), nil
			}
		}
	case qvalue.QValueKindNumeric:
		if v, ok := val.(*sql.Null[decimal.Decimal]); ok {
			if v.Valid {
				return qvalue.QValueNumeric{Val: v.V}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindNumeric), nil
			}
		}
	case qvalue.QValueKindBytes:
		if v, ok := val.(*[]byte); ok && v != nil {
			return qvalue.QValueBytes{Val: *v}, nil
		}

	case qvalue.QValueKindUUID:
		if v, ok := val.(*[]byte); ok && v != nil {
			// convert byte array to string
			uuidVal, err := uuid.FromBytes(*v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse uuid: %v", *v)
			}
			return qvalue.QValueUUID{Val: uuidVal}, nil
		}

	case qvalue.QValueKindJSON:
		vraw := val.(*interface{})
		vstring, ok := (*vraw).(string)
		if !ok {
			slog.Warn("A parsed JSON value was not a string. Likely a null field value")
		}

		if strings.HasPrefix(vstring, "[") {
			// parse the array
			var v []interface{}
			err := json.Unmarshal([]byte(vstring), &v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse json array: %v", vstring)
			}

			// assume all elements in the array are of the same type
			// based on the first element, determine the type
			if len(v) > 0 {
				kind := qvalue.QValueKindString
				switch v[0].(type) {
				case float32:
					kind = qvalue.QValueKindArrayFloat32
				case float64:
					kind = qvalue.QValueKindArrayFloat64
				case int16:
					kind = qvalue.QValueKindArrayInt16
				case int32:
					kind = qvalue.QValueKindArrayInt32
				case int64:
					kind = qvalue.QValueKindArrayInt64
				case string:
					kind = qvalue.QValueKindArrayString
				}

				return toQValueArray(kind, v)
			}
		}

		return qvalue.QValueJSON{Val: vstring}, nil

	case qvalue.QValueKindHStore:
		vraw := val.(*interface{})
		vstring, ok := (*vraw).(string)
		if !ok {
			slog.Warn("A parsed hstore value was not a string. Likely a null field value")
		}

		return qvalue.QValueHStore{Val: vstring}, nil

	case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64,
		qvalue.QValueKindArrayInt16,
		qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64,
		qvalue.QValueKindArrayString:
		return toQValueArray(kind, val)
	}

	// If type is unsupported or doesn't match the specified kind, return error
	return nil, fmt.Errorf("unsupported type %T for kind %s", val, kind)
}

func toQValueArray(kind qvalue.QValueKind, value interface{}) (qvalue.QValue, error) {
	switch kind {
	case qvalue.QValueKindArrayFloat32:
		switch v := value.(type) {
		case []float32:
			return qvalue.QValueArrayFloat32{Val: v}, nil
		case []interface{}:
			float32Array := make([]float32, len(v))
			for i, val := range v {
				float32Array[i] = val.(float32)
			}
			return qvalue.QValueArrayFloat32{Val: float32Array}, nil
		default:
			return nil, fmt.Errorf("failed to parse array float32: %v", value)
		}

	case qvalue.QValueKindArrayFloat64:
		switch v := value.(type) {
		case []float64:
			return qvalue.QValueArrayFloat64{Val: v}, nil
		case []interface{}:
			float64Array := make([]float64, len(v))
			for i, val := range v {
				float64Array[i] = val.(float64)
			}
			return qvalue.QValueArrayFloat64{Val: float64Array}, nil
		default:
			return nil, fmt.Errorf("failed to parse array float64: %v", value)
		}

	case qvalue.QValueKindArrayInt16:
		switch v := value.(type) {
		case []int16:
			return qvalue.QValueArrayInt16{Val: v}, nil
		case []interface{}:
			int16Array := make([]int16, len(v))
			for i, val := range v {
				int16Array[i] = val.(int16)
			}
			return qvalue.QValueArrayInt16{Val: int16Array}, nil
		default:
			return nil, fmt.Errorf("failed to parse array int16: %v", value)
		}

	case qvalue.QValueKindArrayInt32:
		switch v := value.(type) {
		case []int32:
			return qvalue.QValueArrayInt32{Val: v}, nil
		case []interface{}:
			int32Array := make([]int32, len(v))
			for i, val := range v {
				int32Array[i] = val.(int32)
			}
			return qvalue.QValueArrayInt32{Val: int32Array}, nil
		default:
			return nil, fmt.Errorf("failed to parse array int32: %v", value)
		}

	case qvalue.QValueKindArrayInt64:
		switch v := value.(type) {
		case []int64:
			return qvalue.QValueArrayInt64{Val: v}, nil
		case []interface{}:
			int64Array := make([]int64, len(v))
			for i, val := range v {
				int64Array[i] = val.(int64)
			}
			return qvalue.QValueArrayInt64{Val: int64Array}, nil
		default:
			return nil, fmt.Errorf("failed to parse array int64: %v", value)
		}

	case qvalue.QValueKindArrayString:
		switch v := value.(type) {
		case []string:
			return qvalue.QValueArrayString{Val: v}, nil
		case []interface{}:
			stringArray := make([]string, len(v))
			for i, val := range v {
				stringArray[i] = val.(string)
			}
			return qvalue.QValueArrayString{Val: stringArray}, nil
		default:
			return nil, fmt.Errorf("failed to parse array string: %v", value)
		}
	}

	return qvalue.QValueNull(kind), nil
}
