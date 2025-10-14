package connsnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func SnowflakeIdentifierNormalize(identifier string) string {
	// https://www.alberton.info/dbms_identifiers_and_case_sensitivity.html
	// Snowflake follows the SQL standard, but Postgres does the opposite.
	// Ergo, we suffer.
	if utils.IsLower(identifier) {
		return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
	}
	return fmt.Sprintf(`"%s"`, identifier)
}

func SnowflakeQuotelessIdentifierNormalize(identifier string) string {
	if utils.IsLower(identifier) {
		return strings.ToUpper(identifier)
	}
	return identifier
}

func snowflakeSchemaTableNormalize(schemaTable *utils.SchemaTable) string {
	return fmt.Sprintf(`%s.%s`, SnowflakeIdentifierNormalize(schemaTable.Schema),
		SnowflakeIdentifierNormalize(schemaTable.Table))
}

func (c *SnowflakeConnector) columnTypeToQField(ct *sql.ColumnType) (types.QField, error) {
	qvKind, ok := snowflakeTypeToQValueKindMap[ct.DatabaseTypeName()]
	if !ok {
		return types.QField{}, fmt.Errorf("unsupported database type %s", ct.DatabaseTypeName())
	}

	nullable, ok := ct.Nullable()

	return types.QField{
		Name:     ct.Name(),
		Type:     qvKind,
		Nullable: ok && nullable,
	}, nil
}

func (c *SnowflakeConnector) processRows(rows *sql.Rows) (*model.QRecordBatch, error) {
	dbColTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert dbColTypes to QFields
	qfields := make([]types.QField, len(dbColTypes))
	for i, ct := range dbColTypes {
		qfield, err := c.columnTypeToQField(ct)
		if err != nil {
			c.logger.Error(fmt.Sprintf("failed to convert column type %v", ct),
				slog.Any("error", err))
			return nil, err
		}
		qfields[i] = qfield
	}

	values := make([]any, len(dbColTypes))
	for i := range values {
		switch qfields[i].Type {
		case types.QValueKindTimestamp, types.QValueKindTimestampTZ, types.QValueKindTime, types.QValueKindDate:
			var t sql.NullTime
			values[i] = &t
		case types.QValueKindInt32:
			var n sql.NullInt32
			values[i] = &n
		case types.QValueKindInt64:
			var n sql.NullInt64
			values[i] = &n
		case types.QValueKindFloat64:
			var f sql.NullFloat64
			values[i] = &f
		case types.QValueKindBoolean:
			var b sql.NullBool
			values[i] = &b
		case types.QValueKindString, types.QValueKindHStore:
			var s sql.NullString
			values[i] = &s
		case types.QValueKindBytes:
			values[i] = new([]byte)
		case types.QValueKindNumeric:
			var s sql.Null[decimal.Decimal]
			values[i] = &s
		default:
			values[i] = new(any)
		}
	}

	var records [][]types.QValue
	totalRowsProcessed := 0
	const logEveryNumRows = 50000

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		qValues := make([]types.QValue, len(values))
		for i, val := range values {
			qv, err := c.toQValue(qfields[i].Type, val)
			if err != nil {
				c.logger.Error("failed to convert value", slog.Any("error", err))
				return nil, err
			}
			qValues[i] = qv
		}

		records = append(records, qValues)
		totalRowsProcessed += 1

		if totalRowsProcessed%logEveryNumRows == 0 {
			c.logger.Info("processed rows", slog.Int("rows", totalRowsProcessed))
		}
	}

	if err := rows.Err(); err != nil {
		c.logger.Error("failed to iterate over rows", slog.Any("Error", err))
		return nil, err
	}

	return &model.QRecordBatch{
		Schema:  types.NewQRecordSchema(qfields),
		Records: records,
	}, nil
}

func (c *SnowflakeConnector) ExecuteAndProcessQuery(
	ctx context.Context,
	query string,
	args ...any,
) (*model.QRecordBatch, error) {
	rows, err := c.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return c.processRows(rows)
}

func (c *SnowflakeConnector) toQValue(kind types.QValueKind, val any) (types.QValue, error) {
	if val == nil {
		return types.QValueNull(kind), nil
	}
	switch kind {
	case types.QValueKindInt32:
		if v, ok := val.(*sql.NullInt32); ok {
			if v.Valid {
				return types.QValueInt32{Val: v.Int32}, nil
			} else {
				return types.QValueNull(types.QValueKindInt32), nil
			}
		}
	case types.QValueKindInt64:
		if v, ok := val.(*sql.NullInt64); ok {
			if v.Valid {
				return types.QValueInt64{Val: v.Int64}, nil
			} else {
				return types.QValueNull(types.QValueKindInt64), nil
			}
		}
	case types.QValueKindFloat64:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return types.QValueFloat64{Val: v.Float64}, nil
			} else {
				return types.QValueNull(types.QValueKindFloat64), nil
			}
		}
	case types.QValueKindString:
		if v, ok := val.(*sql.NullString); ok {
			if v.Valid {
				return types.QValueString{Val: v.String}, nil
			} else {
				return types.QValueNull(types.QValueKindString), nil
			}
		}
	case types.QValueKindBoolean:
		if v, ok := val.(*sql.NullBool); ok {
			if v.Valid {
				return types.QValueBoolean{Val: v.Bool}, nil
			} else {
				return types.QValueNull(types.QValueKindBoolean), nil
			}
		}
	case types.QValueKindTimestamp:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return types.QValueTimestamp{Val: t.Time}, nil
			} else {
				return types.QValueNull(kind), nil
			}
		}
	case types.QValueKindTimestampTZ:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return types.QValueTimestampTZ{Val: t.Time}, nil
			} else {
				return types.QValueNull(kind), nil
			}
		}
	case types.QValueKindDate:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				return types.QValueDate{Val: t.Time}, nil
			} else {
				return types.QValueNull(kind), nil
			}
		}
	case types.QValueKindTime:
		if t, ok := val.(*sql.NullTime); ok {
			if t.Valid {
				tt := t.Time
				h, m, s := tt.Clock()
				return types.QValueTime{
					Val: time.Duration(h)*time.Hour +
						time.Duration(m)*time.Minute +
						time.Duration(s)*time.Second +
						time.Duration(tt.Nanosecond()),
				}, nil
			} else {
				return types.QValueNull(kind), nil
			}
		}
	case types.QValueKindNumeric:
		if v, ok := val.(*sql.Null[decimal.Decimal]); ok {
			if v.Valid {
				return types.QValueNumeric{Val: v.V}, nil
			} else {
				return types.QValueNull(types.QValueKindNumeric), nil
			}
		}
	case types.QValueKindBytes:
		if v, ok := val.(*[]byte); ok && v != nil {
			return types.QValueBytes{Val: *v}, nil
		}

	case types.QValueKindJSON:
		vraw := val.(*any)
		vstring, ok := (*vraw).(string)
		if !ok {
			c.logger.Warn("A parsed JSON value was not a string. Likely a null field value")
		}

		return types.QValueJSON{Val: vstring}, nil
	}

	// If type is unsupported or doesn't match the specified kind, return error
	return nil, fmt.Errorf("unsupported type %T for kind %s", val, kind)
}

func (c *SnowflakeConnector) CountRows(ctx context.Context, schemaName string, tableName string) (int64, error) {
	var count int64
	err := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+schemaName+"."+tableName).Scan(&count)
	return count, err
}

func (c *SnowflakeConnector) CountNonNullRows(
	ctx context.Context,
	schemaName string,
	tableName string,
	columnName string,
) (int64, error) {
	var count int64
	err := c.QueryRowContext(ctx,
		"SELECT COUNT(CASE WHEN "+columnName+" IS NOT NULL THEN 1 END) AS non_null_count FROM "+schemaName+"."+tableName).Scan(&count)
	return count, err
}

func (c *SnowflakeConnector) CountSRIDs(
	ctx context.Context,
	schemaName string,
	tableName string,
	columnName string,
) (int64, error) {
	var count int64
	err := c.QueryRowContext(ctx, "SELECT COUNT(CASE WHEN ST_SRID("+columnName+
		") <> 0 THEN 1 END) AS not_zero FROM "+schemaName+"."+tableName).Scan(&count)
	return count, err
}

// returns true if any of the columns are null in value
func (c *SnowflakeConnector) CheckNull(ctx context.Context, schema string, tableName string, colNames []string) (bool, error) {
	var count int64
	joinedString := strings.Join(colNames, " is null or ") + " is null"

	if err := c.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s", schema, tableName, joinedString),
	).Scan(&count); err != nil {
		return false, err
	}

	return count == 0, nil
}
