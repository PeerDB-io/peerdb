package peersql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"

	"go.temporal.io/sdk/activity"
)

type SQLQueryExecutor interface {
	ConnectionActive() error
	Close() error

	CreateSchema(schemaName string) error
	DropSchema(schemaName string) error
	CheckSchemaExists(schemaName string) (bool, error)
	RecreateSchema(schemaName string) error

	CreateTable(schema *model.QRecordSchema, schemaName string, tableName string) error
	CountRows(schemaName string, tableName string) (int64, error)

	ExecuteAndProcessQuery(query string, args ...interface{}) (*model.QRecordBatch, error)
	NamedExecuteAndProcessQuery(query string, arg interface{}) (*model.QRecordBatch, error)
	ExecuteQuery(query string, args ...interface{}) error
	NamedExec(query string, arg interface{}) (sql.Result, error)
}

type GenericSQLQueryExecutor struct {
	ctx                context.Context
	db                 *sqlx.DB
	dbtypeToQValueKind map[string]qvalue.QValueKind
	qvalueKindToDBType map[qvalue.QValueKind]string
	logger             slog.Logger
}

func NewGenericSQLQueryExecutor(
	ctx context.Context,
	db *sqlx.DB,
	dbtypeToQValueKind map[string]qvalue.QValueKind,
	qvalueKindToDBType map[qvalue.QValueKind]string,
) *GenericSQLQueryExecutor {
	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &GenericSQLQueryExecutor{
		ctx:                ctx,
		db:                 db,
		dbtypeToQValueKind: dbtypeToQValueKind,
		qvalueKindToDBType: qvalueKindToDBType,
		logger:             *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
	}
}

func (g *GenericSQLQueryExecutor) ConnectionActive() bool {
	err := g.db.PingContext(g.ctx)
	return err == nil
}

func (g *GenericSQLQueryExecutor) Close() error {
	return g.db.Close()
}

func (g *GenericSQLQueryExecutor) CreateSchema(schemaName string) error {
	_, err := g.db.ExecContext(g.ctx, "CREATE SCHEMA "+schemaName)
	return err
}

func (g *GenericSQLQueryExecutor) DropSchema(schemaName string) error {
	_, err := g.db.ExecContext(g.ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
	return err
}

// the SQL query this function executes appears to be MySQL/MariaDB specific.
func (g *GenericSQLQueryExecutor) CheckSchemaExists(schemaName string) (bool, error) {
	var exists pgtype.Bool
	// use information schemata to check if schema exists
	err := g.db.QueryRowxContext(g.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", schemaName).Scan(&exists)
	return exists.Bool, err
}

func (g *GenericSQLQueryExecutor) RecreateSchema(schemaName string) error {
	err := g.DropSchema(schemaName)
	if err != nil {
		return fmt.Errorf("failed to drop schema: %w", err)
	}

	err = g.CreateSchema(schemaName)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	return nil
}

func (g *GenericSQLQueryExecutor) CreateTable(schema *model.QRecordSchema, schemaName string, tableName string) error {
	fields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		dbType, ok := g.qvalueKindToDBType[field.Type]
		if !ok {
			return fmt.Errorf("unsupported qvalue type %s", field.Type)
		}
		fields = append(fields, fmt.Sprintf(`"%s" %s`, field.Name, dbType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(fields, ", "))
	fmt.Printf("creating table %s.%s with command %s\n", schemaName, tableName, command)

	_, err := g.db.ExecContext(g.ctx, command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (g *GenericSQLQueryExecutor) CountRows(schemaName string, tableName string) (int64, error) {
	var count pgtype.Int8
	err := g.db.QueryRowx("SELECT COUNT(*) FROM " + schemaName + "." + tableName).Scan(&count)
	return count.Int64, err
}

func (g *GenericSQLQueryExecutor) CountNonNullRows(
	schemaName string,
	tableName string,
	columnName string,
) (int64, error) {
	var count pgtype.Int8
	err := g.db.QueryRowx("SELECT COUNT(CASE WHEN " + columnName +
		" IS NOT NULL THEN 1 END) AS non_null_count FROM " + schemaName + "." + tableName).Scan(&count)
	return count.Int64, err
}

func (g *GenericSQLQueryExecutor) columnTypeToQField(ct *sql.ColumnType) (model.QField, error) {
	qvKind, ok := g.dbtypeToQValueKind[ct.DatabaseTypeName()]
	if !ok {
		return model.QField{}, fmt.Errorf("unsupported database type %s", ct.DatabaseTypeName())
	}

	nullable, ok := ct.Nullable()

	return model.QField{
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
	qfields := make([]model.QField, len(dbColTypes))
	for i, ct := range dbColTypes {
		qfield, err := g.columnTypeToQField(ct)
		if err != nil {
			g.logger.Error(fmt.Sprintf("failed to convert column type %v", ct),
				slog.Any("error", err))
			return nil, err
		}
		qfields[i] = qfield
	}

	var records []model.QRecord
	totalRowsProcessed := 0
	const heartBeatNumRows = 25000

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
			case qvalue.QValueKindString:
				var s sql.NullString
				values[i] = &s
			case qvalue.QValueKindBytes, qvalue.QValueKindBit:
				values[i] = new([]byte)
			case qvalue.QValueKindNumeric:
				var s sql.NullString
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

		// Create a QRecord
		record := model.NewQRecord(len(qValues))
		for i, qv := range qValues {
			record.Set(i, qv)
		}

		records = append(records, record)
		totalRowsProcessed += 1

		if totalRowsProcessed%heartBeatNumRows == 0 {
			activity.RecordHeartbeat(g.ctx, fmt.Sprintf("processed %d rows", totalRowsProcessed))
		}
	}

	if err := rows.Err(); err != nil {
		g.logger.Error("failed to iterate over rows", slog.Any("Error", err))
		return nil, err
	}

	// Return a QRecordBatch
	return &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     model.NewQRecordSchema(qfields),
	}, nil
}

func (g *GenericSQLQueryExecutor) ExecuteAndProcessQuery(
	query string, args ...interface{},
) (*model.QRecordBatch, error) {
	rows, err := g.db.QueryxContext(g.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return g.processRows(rows)
}

func (g *GenericSQLQueryExecutor) NamedExecuteAndProcessQuery(
	query string, arg interface{},
) (*model.QRecordBatch, error) {
	rows, err := g.db.NamedQueryContext(g.ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return g.processRows(rows)
}

func (g *GenericSQLQueryExecutor) ExecuteQuery(query string, args ...interface{}) error {
	_, err := g.db.ExecContext(g.ctx, query, args...)
	return err
}

func (g *GenericSQLQueryExecutor) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return g.db.NamedExecContext(g.ctx, query, arg)
}

// returns true if any of the columns are null in value
func (g *GenericSQLQueryExecutor) CheckNull(schema string, tableName string, colNames []string) (bool, error) {
	var count pgtype.Int8
	joinedString := strings.Join(colNames, " is null or ") + " is null"
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s",
		schema, tableName, joinedString)

	err := g.db.QueryRowxContext(g.ctx, query).Scan(&count)
	if err != nil {
		return false, err
	}

	return count.Int64 == 0, nil
}

func toQValue(kind qvalue.QValueKind, val interface{}) (qvalue.QValue, error) {
	switch kind {
	case qvalue.QValueKindInt32:
		if v, ok := val.(*sql.NullInt32); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: v.Int32}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindInt32, Value: nil}, nil
			}
		}
	case qvalue.QValueKindInt64:
		if v, ok := val.(*sql.NullInt64); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: v.Int64}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindInt64, Value: nil}, nil
			}
		}
	case qvalue.QValueKindFloat32:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: float32(v.Float64)}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindFloat32, Value: nil}, nil
			}
		}
	case qvalue.QValueKindFloat64:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: v.Float64}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindFloat64, Value: nil}, nil
			}
		}
	case qvalue.QValueKindString:
		if v, ok := val.(*sql.NullString); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindString, Value: v.String}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindString, Value: nil}, nil
			}
		}
	case qvalue.QValueKindBoolean:
		if v, ok := val.(*sql.NullBool); ok {
			if v.Valid {
				return qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: v.Bool}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindBoolean, Value: nil}, nil
			}
		}
	case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindDate,
		qvalue.QValueKindTime, qvalue.QValueKindTimeTZ:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return qvalue.QValue{
					Kind:  kind,
					Value: t.Time,
				}, nil
			} else {
				return qvalue.QValue{
					Kind:  kind,
					Value: nil,
				}, nil
			}
		}
	case qvalue.QValueKindNumeric:
		if v, ok := val.(*sql.NullString); ok {
			if v.Valid {
				numeric := new(big.Rat)
				if _, ok := numeric.SetString(v.String); !ok {
					return qvalue.QValue{}, fmt.Errorf("failed to parse numeric: %v", v.String)
				}
				return qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: numeric}, nil
			} else {
				return qvalue.QValue{Kind: qvalue.QValueKindNumeric, Value: nil}, nil
			}
		}
	case qvalue.QValueKindBytes, qvalue.QValueKindBit:
		if v, ok := val.(*[]byte); ok && v != nil {
			return qvalue.QValue{Kind: kind, Value: *v}, nil
		}

	case qvalue.QValueKindUUID:
		if v, ok := val.(*[]byte); ok && v != nil {
			// convert byte array to string
			uuidVal, err := uuid.FromBytes(*v)
			if err != nil {
				return qvalue.QValue{}, fmt.Errorf("failed to parse uuid: %v", *v)
			}
			return qvalue.QValue{Kind: qvalue.QValueKindString, Value: uuidVal.String()}, nil
		}

		if v, ok := val.(*[16]byte); ok && v != nil {
			return qvalue.QValue{Kind: qvalue.QValueKindString, Value: *v}, nil
		}

	case qvalue.QValueKindJSON:
		vraw := val.(*interface{})
		vstring := (*vraw).(string)

		if strings.HasPrefix(vstring, "[") {
			// parse the array
			var v []interface{}
			err := json.Unmarshal([]byte(vstring), &v)
			if err != nil {
				return qvalue.QValue{}, fmt.Errorf("failed to parse json array: %v", vstring)
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

		return qvalue.QValue{Kind: qvalue.QValueKindJSON, Value: vstring}, nil

	case qvalue.QValueKindHStore:
		// TODO fix this.
		return qvalue.QValue{Kind: qvalue.QValueKindHStore, Value: val}, nil

	case qvalue.QValueKindArrayFloat32, qvalue.QValueKindArrayFloat64,
		qvalue.QValueKindArrayInt32, qvalue.QValueKindArrayInt64,
		qvalue.QValueKindArrayString:
		// TODO fix this.
		return toQValueArray(kind, val)
	}

	// If type is unsupported or doesn't match the specified kind, return error
	return qvalue.QValue{}, fmt.Errorf("unsupported type %T for kind %s", val, kind)
}

func toQValueArray(kind qvalue.QValueKind, value interface{}) (qvalue.QValue, error) {
	var result interface{}
	switch kind {
	case qvalue.QValueKindArrayFloat32:
		switch v := value.(type) {
		case []float32:
			result = v
		case []interface{}:
			float32Array := make([]float32, len(v))
			for i, val := range v {
				float32Array[i] = val.(float32)
			}
			result = float32Array
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse array float32: %v", value)
		}

	case qvalue.QValueKindArrayFloat64:
		switch v := value.(type) {
		case []float64:
			result = v
		case []interface{}:
			float64Array := make([]float64, len(v))
			for i, val := range v {
				float64Array[i] = val.(float64)
			}
			result = float64Array
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse array float64: %v", value)
		}

	case qvalue.QValueKindArrayInt32:
		switch v := value.(type) {
		case []int32:
			result = v
		case []interface{}:
			int32Array := make([]int32, len(v))
			for i, val := range v {
				int32Array[i] = val.(int32)
			}
			result = int32Array
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse array int32: %v", value)
		}

	case qvalue.QValueKindArrayInt64:
		switch v := value.(type) {
		case []int64:
			result = v
		case []interface{}:
			int64Array := make([]int64, len(v))
			for i, val := range v {
				int64Array[i] = val.(int64)
			}
			result = int64Array
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse array int64: %v", value)
		}

	case qvalue.QValueKindArrayString:
		switch v := value.(type) {
		case []string:
			result = v
		case []interface{}:
			stringArray := make([]string, len(v))
			for i, val := range v {
				stringArray[i] = val.(string)
			}
			result = stringArray
		default:
			return qvalue.QValue{}, fmt.Errorf("failed to parse array string: %v", value)
		}
	}

	return qvalue.QValue{Kind: kind, Value: result}, nil
}
