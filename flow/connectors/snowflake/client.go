package connsnowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
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
	query := fmt.Sprintf("SHOW SCHEMAS LIKE '%s'", schema)
	err := s.conn.GetContext(s.ctx, &exists, query)
	if err != nil {
		return false, fmt.Errorf("failed to query schemas: %w", err)
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

func toQValue(val interface{}) (model.QValue, error) {
	// Based on the real type of the interface{}, we create a model.QValue
	switch v := val.(type) {
	case int, int32:
		return model.QValue{Kind: model.QValueKindInt32, Value: v}, nil
	case int64:
		return model.QValue{Kind: model.QValueKindInt64, Value: v}, nil
	case float32:
		return model.QValue{Kind: model.QValueKindFloat32, Value: v}, nil
	case float64:
		return model.QValue{Kind: model.QValueKindFloat64, Value: v}, nil
	case string:
		return model.QValue{Kind: model.QValueKindString, Value: v}, nil
	case bool:
		return model.QValue{Kind: model.QValueKindBoolean, Value: v}, nil
	case time.Time:
		val, err := model.NewExtendedTime(v, model.DateTimeKindType, "")
		if err != nil {
			return model.QValue{}, fmt.Errorf("failed to create ExtendedTime: %w", err)
		}
		return model.QValue{
			Kind:  model.QValueKindETime,
			Value: val,
		}, nil
	case []byte:
		return model.QValue{Kind: model.QValueKindBytes, Value: v}, nil
	default:
		// If type is unsupported, return error
		return model.QValue{}, fmt.Errorf("[snowflakeclient] unsupported type %T", v)
	}
}

// DatabaseTypeNameToQValueKind converts a database type name to a QValueKind.
func DatabaseTypeNameToQValueKind(name string) (model.QValueKind, error) {
	switch name {
	case "INT":
		return model.QValueKindInt32, nil
	case "BIGINT":
		return model.QValueKindInt64, nil
	case "FLOAT":
		return model.QValueKindFloat32, nil
	case "DOUBLE":
		return model.QValueKindFloat64, nil
	case "VARCHAR", "CHAR", "TEXT":
		return model.QValueKindString, nil
	case "BOOLEAN":
		return model.QValueKindBoolean, nil
	case "DATETIME", "TIMESTAMP":
		return model.QValueKindETime, nil
	case "BLOB", "BYTEA":
		return model.QValueKindBytes, nil
	default:
		// If type is unsupported, return an error
		return "", fmt.Errorf("unsupported database type name: %s", name)
	}
}

func (s *SnowflakeClient) ExecuteAndProcessQuery(query string) (*model.QRecordBatch, error) {
	rows, err := s.conn.QueryContext(s.ctx, query)
	if err != nil {
		fmt.Printf("failed to run command: %v\n", err)
		return nil, fmt.Errorf("failed to run command: %w", err)
	}
	defer rows.Close()

	var records []*model.QRecord

	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		qValues := make([]model.QValue, len(values))
		for i, val := range values {
			qv, err := toQValue(val)
			if err != nil {
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
		return nil, err
	}

	dbColTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	colTypes := make([]model.QValueKind, len(dbColTypes))
	colNames := make([]string, len(dbColTypes))

	// convert colTypes to QValueKind
	for i, ct := range dbColTypes {
		colTypes[i], err = DatabaseTypeNameToQValueKind(ct.DatabaseTypeName())
		if err != nil {
			return nil, err
		}

		colNames[i] = ct.Name()
	}

	// Return a QRecordBatch
	return &model.QRecordBatch{
		NumRecords: uint32(len(records)),
		Records:    records,
		Schema:     model.NewQRecordSchema(colNames, colTypes),
	}, nil
}
