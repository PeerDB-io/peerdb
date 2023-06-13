package connsnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	util "github.com/PeerDB-io/peer-flow/utils"
)

type SnowflakeClient struct {
	// ctx is the context.
	ctx context.Context
	// config is the Snowflake config.
	Config *protos.SnowflakeConfig
	// connection to Snowflake
	conn *sqlx.DB
}

func NewSnowflakeClient(ctx context.Context, config *protos.SnowflakeConfig) (*SnowflakeClient, error) {
	privateKey, err := util.DecodePKCS8PrivateKey([]byte(config.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	snowflakeConfig := gosnowflake.Config{
		Account:          config.AccountId,
		User:             config.Username,
		Authenticator:    gosnowflake.AuthTypeJwt,
		PrivateKey:       privateKey,
		Database:         config.Database,
		Warehouse:        config.Warehouse,
		Role:             config.Role,
		RequestTimeout:   time.Duration(config.QueryTimeout) * time.Second,
		DisableTelemetry: true,
	}

	snowflakeConfigDSN, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get DSN from Snowflake config: %w", err)
	}

	database, err := sqlx.Open("snowflake", snowflakeConfigDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	err = database.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Snowflake peer: %w", err)
	}

	return &SnowflakeClient{
		ctx:    ctx,
		Config: config,
		conn:   database,
	}, nil
}

// ConnectionActive checks if the connection is active.
func (s *SnowflakeClient) ConnectionActive() bool {
	return s.conn.PingContext(s.ctx) == nil
}

// add a Close() method to SnowflakeClient
func (s *SnowflakeClient) Close() error {
	return s.conn.Close()
}

// schemaExists checks if the schema exists.
func (s *SnowflakeClient) schemaExists(schema string) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'", schema)
	err := s.conn.QueryRowContext(s.ctx, query).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to query schema: %w", err)
	}

	return exists, nil
}

// RecreateSchema recreates the schema, i.e., drops it if exists and creates it again.
func (s *SnowflakeClient) RecreateSchema(schema string) error {
	exists, err := s.schemaExists(schema)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %w", schema, err)
	}

	if exists {
		stmt := fmt.Sprintf("DROP SCHEMA %s", schema)
		_, err := s.conn.ExecContext(s.ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
	}

	stmt := fmt.Sprintf("CREATE SCHEMA %s", schema)
	_, err = s.conn.ExecContext(s.ctx, stmt)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	fmt.Printf("created schema %s successfully\n", schema)
	return nil
}

// DropSchema drops the schema.
func (s *SnowflakeClient) DropSchema(schema string) error {
	exists, err := s.schemaExists(schema)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %w", schema, err)
	}

	if exists {
		stmt := fmt.Sprintf("DROP SCHEMA %s", schema)
		_, err := s.conn.ExecContext(s.ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to drop schema: %w", err)
		}
	}

	return nil
}

// RunCommand runs the given command.
func (s *SnowflakeClient) RunCommand(command string) error {
	_, err := s.conn.ExecContext(s.ctx, command)
	if err != nil {
		return fmt.Errorf("failed to run command: %w", err)
	}

	return nil
}

// CountRows returns the number of rows in the given table.
func (s *SnowflakeClient) CountRows(schema string, tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName)
	err := s.conn.GetContext(s.ctx, &count, query)
	if err != nil {
		return 0, fmt.Errorf("failed to run command: %w", err)
	}

	return count, nil
}

/*
if the function errors or there are nulls, the function returns false
else true
*/
func (s *SnowflakeClient) CheckNull(schema string, tableName string, colNames []string) (bool, error) {
	var count int
	joinedString := strings.Join(colNames, " is null or ") + " is null"
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s",
		schema, tableName, joinedString)

	err := s.conn.GetContext(s.ctx, &count, query)
	if err != nil {
		return false, fmt.Errorf("failed to run command: %w", err)
	}
	if count > 0 {
		return false, nil
	}
	return true, nil
}

func toQValue(kind model.QValueKind, val interface{}) (model.QValue, error) {
	switch kind {
	case model.QValueKindInt32:
		if v, ok := val.(*int); ok && v != nil {
			return model.QValue{Kind: model.QValueKindInt32, Value: *v}, nil
		}
	case model.QValueKindInt64:
		if v, ok := val.(*int64); ok && v != nil {
			return model.QValue{Kind: model.QValueKindInt64, Value: *v}, nil
		}
	case model.QValueKindFloat32:
		if v, ok := val.(*float32); ok && v != nil {
			return model.QValue{Kind: model.QValueKindFloat32, Value: *v}, nil
		}
	case model.QValueKindFloat64:
		if v, ok := val.(*float64); ok && v != nil {
			return model.QValue{Kind: model.QValueKindFloat64, Value: *v}, nil
		}
	case model.QValueKindString:
		if v, ok := val.(*string); ok && v != nil {
			return model.QValue{Kind: model.QValueKindString, Value: *v}, nil
		}
	case model.QValueKindBoolean:
		if v, ok := val.(*bool); ok && v != nil {
			return model.QValue{Kind: model.QValueKindBoolean, Value: *v}, nil
		}
	case model.QValueKindNumeric:
		// convert string to big.Rat
		if v, ok := val.(*string); ok && v != nil {
			//nolint:gosec
			ratVal, ok := new(big.Rat).SetString(*v)
			if !ok {
				return model.QValue{}, fmt.Errorf("failed to convert string to big.Rat: %s", *v)
			}
			return model.QValue{
				Kind:  model.QValueKindNumeric,
				Value: ratVal,
			}, nil
		}
	case model.QValueKindETime:
		if v, ok := val.(*time.Time); ok && v != nil {
			etimeVal, err := model.NewExtendedTime(*v, model.DateTimeKindType, "")
			if err != nil {
				return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
			}
			return model.QValue{
				Kind:  model.QValueKindETime,
				Value: etimeVal,
			}, nil
		}
	case model.QValueKindBytes:
		if v, ok := val.(*[]byte); ok && v != nil {
			return model.QValue{Kind: model.QValueKindBytes, Value: *v}, nil
		}
	}

	// If type is unsupported or doesn't match the specified kind, return error
	return model.QValue{}, fmt.Errorf("[snowflakeclient] unsupported type %T for kind %s", val, kind)
}

// databaseTypeNameToQValueKind converts a database type name to a QValueKind.
func databaseTypeNameToQValueKind(name string) (model.QValueKind, error) {
	switch name {
	case "INT":
		return model.QValueKindInt32, nil
	case "BIGINT":
		return model.QValueKindInt64, nil
	case "FLOAT":
		return model.QValueKindFloat32, nil
	case "DOUBLE", "REAL":
		return model.QValueKindFloat64, nil
	case "VARCHAR", "CHAR", "TEXT":
		return model.QValueKindString, nil
	case "BOOLEAN":
		return model.QValueKindBoolean, nil
	case "DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ":
		return model.QValueKindETime, nil
	case "BLOB", "BYTEA", "BINARY":
		return model.QValueKindBytes, nil
	case "FIXED", "NUMBER":
		return model.QValueKindNumeric, nil
	default:
		// If type is unsupported, return an error
		return "", fmt.Errorf("unsupported database type name: %s", name)
	}
}

func columnTypeToQField(ct *sql.ColumnType) (*model.QField, error) {
	qvKind, err := databaseTypeNameToQValueKind(ct.DatabaseTypeName())
	if err != nil {
		return nil, err
	}

	nullable, ok := ct.Nullable()

	return &model.QField{
		Name:     ct.Name(),
		Type:     qvKind,
		Nullable: ok && nullable,
	}, nil
}

func (s *SnowflakeClient) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	rows, err := s.conn.QueryContext(s.ctx, query)
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
		qfield, err := columnTypeToQField(ct)
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
			case model.QValueKindETime:
				values[i] = new(time.Time)
			case model.QValueKindInt16:
				values[i] = new(int16)
			case model.QValueKindInt32:
				values[i] = new(int32)
			case model.QValueKindInt64:
				values[i] = new(int64)
			case model.QValueKindFloat32:
				values[i] = new(float32)
			case model.QValueKindFloat64:
				values[i] = new(float64)
			case model.QValueKindBoolean:
				values[i] = new(bool)
			case model.QValueKindString:
				values[i] = new(string)
			case model.QValueKindBytes:
				values[i] = new([]byte)
			case model.QValueKindNumeric:
				values[i] = new(string)
			default:
				values[i] = new(interface{})
			}
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		qValues := make([]model.QValue, len(values))
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

func (s *SnowflakeClient) CreateTable(schema *model.QRecordSchema, schemaName string, tableName string) error {
	var fields []string
	for _, field := range schema.Fields {
		snowflakeType, err := qValueKindToSnowflakeColTypeString(field.Type)
		if err != nil {
			return err
		}
		fields = append(fields, fmt.Sprintf("%s %s", field.Name, snowflakeType))
	}

	command := fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(fields, ", "))
	fmt.Printf("creating table %s.%s with command %s\n", schemaName, tableName, command)

	_, err := s.conn.ExecContext(s.ctx, command)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func qValueKindToSnowflakeColTypeString(val model.QValueKind) (string, error) {
	switch val {
	case model.QValueKindInt32, model.QValueKindInt64:
		return "INT", nil
	case model.QValueKindFloat32, model.QValueKindFloat64:
		return "FLOAT", nil
	case model.QValueKindString:
		return "STRING", nil
	case model.QValueKindBoolean:
		return "BOOLEAN", nil
	case model.QValueKindETime:
		return "TIMESTAMP_LTZ", nil
	case model.QValueKindBytes:
		return "BINARY", nil
	case model.QValueKindNumeric:
		return "NUMERIC(38,32)", nil
	default:
		return "", fmt.Errorf("unsupported QValueKind: %v", val)
	}
}
