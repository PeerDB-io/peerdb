package conngeneric

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type GenericQueryExecutor struct {
	ctx                context.Context
	db                 *sqlx.DB
	dbtypeToQValueKind map[string]qvalue.QValueKind
	qvalueKindToDBType map[qvalue.QValueKind]string
}

func NewGenericQueryExecutor(ctx context.Context, db *sqlx.DB) *GenericQueryExecutor {
	return &GenericQueryExecutor{
		ctx: ctx,
		db:  db,
	}
}

func (g *GenericQueryExecutor) ConnectionActive() bool {
	return g.db.PingContext(g.ctx) == nil
}

func (g *GenericQueryExecutor) Close() error {
	return g.db.Close()
}

func (g *GenericQueryExecutor) schemaExists(schema string) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'", schema)
	err := g.db.QueryRowContext(g.ctx, query).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to query schema: %w", err)
	}

	return exists, nil
}

func (g *GenericQueryExecutor) RecreateSchema(schema string) error {
	exists, err := g.schemaExists(schema)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %w", schema, err)
	}

	if exists {
		stmt := fmt.Sprintf("DROP SCHEMA %s", schema)
		_, err := g.db.ExecContext(g.ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
	}

	stmt := fmt.Sprintf("CREATE SCHEMA %s", schema)
	_, err = g.db.ExecContext(g.ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	fmt.Printf("created schema %s successfully\n", schema)
	return nil
}

func (g *GenericQueryExecutor) DropSchema(schema string) error {
	fmt.Println("dropping schema: ", schema)
	exists, err := g.schemaExists(schema)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %w", schema, err)
	}

	if exists {
		stmt := fmt.Sprintf("DROP SCHEMA %s", schema)
		_, err := g.db.ExecContext(g.ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
	}

	return nil
}

func (g *GenericQueryExecutor) RunCommand(command string) error {
	_, err := g.db.ExecContext(g.ctx, command)
	if err != nil {
		return fmt.Errorf("failed to run command: %w", err)
	}

	return nil
}

func (g *GenericQueryExecutor) CountRows(schema string, tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName)
	err := g.db.GetContext(g.ctx, &count, query)
	if err != nil {
		return 0, fmt.Errorf("failed to run command: %w", err)
	}

	return count, nil
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
				//nolint:gosec
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
	}

	// If type is unsupported or doesn't match the specified kind, return error
	return qvalue.QValue{}, fmt.Errorf("[snowflakeclient] unsupported type %T for kind %s", val, kind)
}

func (g *GenericQueryExecutor) columnTypeToQField(ct *sql.ColumnType) (*model.QField, error) {
	qvKind, ok := g.dbtypeToQValueKind[ct.DatabaseTypeName()]
	if !ok {
		return nil, fmt.Errorf("unsupported type %s", ct.DatabaseTypeName())
	}

	nullable, ok := ct.Nullable()

	return &model.QField{
		Name:     ct.Name(),
		Type:     qvKind,
		Nullable: ok && nullable,
	}, nil
}

func (g *GenericQueryExecutor) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	rows, err := g.db.QueryContext(g.ctx, query)
	if err != nil {
		fmt.Printf("failed to run command: %v\n", err)
		return nil, fmt.Errorf("failed to run command: %w", err)
	}
	defer rows.Close()

	dbColTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert dbColTypes to QFields
	qfields := make([]*model.QField, len(dbColTypes))
	for i, ct := range dbColTypes {
		qfield, err := g.columnTypeToQField(ct)
		if err != nil {
			log.Errorf("failed to convert column type %v: %v", ct, err)
			return nil, err
		}
		qfields[i] = qfield
	}

	var records []*model.QRecord

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
				log.Errorf("failed to convert value: %v", err)
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
	}

	if err := rows.Err(); err != nil {
		log.Errorf("failed to iterate over rows: %v", err)
		return nil, err
	}

	// Return a QRecordBatch
	return &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     model.NewQRecordSchema(qfields),
	}, nil
}

func (g *GenericQueryExecutor) CreateTable(schema *model.QRecordSchema, schemaName string, tableName string) error {
	var fields []string
	for _, field := range schema.Fields {
		snowflakeType := qValueKindToSnowflakeType(string(field.Type))
		fields = append(fields, fmt.Sprintf(`"%s" %s`, field.Name, snowflakeType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(fields, ", "))
	fmt.Printf("creating table %s.%s with command %s\n", schemaName, tableName, command)

	_, err := g.db.ExecContext(g.ctx, command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
