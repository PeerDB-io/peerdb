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
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
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

func (c *SnowflakeConnector) columnTypeToQField(ct *sql.ColumnType) (qvalue.QField, error) {
	qvKind, ok := snowflakeTypeToQValueKindMap[ct.DatabaseTypeName()]
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

func (c *SnowflakeConnector) processRows(rows *sql.Rows) (*model.QRecordBatch, error) {
	dbColTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert dbColTypes to QFields
	qfields := make([]qvalue.QField, len(dbColTypes))
	for i, ct := range dbColTypes {
		qfield, err := c.columnTypeToQField(ct)
		if err != nil {
			c.logger.Error(fmt.Sprintf("failed to convert column type %v", ct),
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

		values := make([]any, len(columns))
		for i := range values {
			switch qfields[i].Type {
			case qvalue.QValueKindTimestamp, qvalue.QValueKindTimestampTZ, qvalue.QValueKindTime, qvalue.QValueKindDate:
				var t sql.NullTime
				values[i] = &t
			case qvalue.QValueKindInt32:
				var n sql.NullInt32
				values[i] = &n
			case qvalue.QValueKindInt64:
				var n sql.NullInt64
				values[i] = &n
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
			default:
				values[i] = new(any)
			}
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		qValues := make([]qvalue.QValue, len(values))
		for i, val := range values {
			qv, err := toQValue(qfields[i].Type, val)
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
		Schema:  qvalue.NewQRecordSchema(qfields),
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

func toQValue(kind qvalue.QValueKind, val any) (qvalue.QValue, error) {
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
	case qvalue.QValueKindFloat64:
		if v, ok := val.(*sql.NullFloat64); ok {
			if v.Valid {
				return qvalue.QValueFloat64{Val: v.Float64}, nil
			} else {
				return qvalue.QValueNull(qvalue.QValueKindFloat64), nil
			}
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
				return qvalue.QValueTime{
					Val: time.Date(1970, time.January, 1, tt.Hour(), tt.Minute(), tt.Second(), tt.Nanosecond(), time.UTC),
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

	case qvalue.QValueKindJSON:
		vraw := val.(*any)
		vstring, ok := (*vraw).(string)
		if !ok {
			slog.Warn("A parsed JSON value was not a string. Likely a null field value")
		}

		return qvalue.QValueJSON{Val: vstring}, nil
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
