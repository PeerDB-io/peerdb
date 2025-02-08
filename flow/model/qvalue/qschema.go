package qvalue

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
)

type QField struct {
	Name      string
	Type      QValueKind
	Precision int16
	Scale     int16
	Nullable  bool
}

type QRecordSchema struct {
	Fields []QField
}

// NewQRecordSchema creates a new QRecordSchema.
func NewQRecordSchema(fields []QField) QRecordSchema {
	return QRecordSchema{Fields: fields}
}

// EqualNames returns true if the field names are equal.
func (q QRecordSchema) EqualNames(other QRecordSchema) bool {
	if len(q.Fields) != len(other.Fields) {
		return false
	}

	for i, field := range q.Fields {
		if !strings.EqualFold(field.Name, other.Fields[i].Name) {
			return false
		}
	}

	return true
}

// GetColumnNames returns a slice of column names.
func (q QRecordSchema) GetColumnNames() []string {
	names := make([]string, 0, len(q.Fields))
	for _, field := range q.Fields {
		names = append(names, field.Name)
	}
	return names
}

func (q QField) getClickHouseTypeForNumericField(ctx context.Context, env map[string]string) (string, error) {
	if q.Precision == 0 && q.Scale == 0 {
		numericAsStringEnabled, err := peerdbenv.PeerDBEnableClickHouseNumericAsString(ctx, env)
		if err != nil {
			return "", err
		}
		if numericAsStringEnabled {
			return "String", nil
		}
	} else if q.Precision > datatypes.PeerDBClickHouseMaxPrecision {
		return "String", nil
	}
	return fmt.Sprintf("Decimal(%d, %d)", q.Precision, q.Scale), nil
}

// SEE ALSO: qvalue/kind.go ToDWHColumnType
func (q QField) ToDWHColumnType(
	ctx context.Context, env map[string]string, dwhType protos.DBType, nullableEnabled bool,
) (string, error) {
	var colType string
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if val, ok := QValueKindToSnowflakeTypeMap[q.Type]; ok {
			colType = val
		} else if q.Type == QValueKindNumeric {
			colType = fmt.Sprintf("NUMERIC(%d,%d)", q.Precision, q.Scale)
		} else {
			colType = "STRING"
		}
		if nullableEnabled && !q.Nullable {
			colType += " NOT NULL"
		}
	case protos.DBType_CLICKHOUSE:
		if val, ok := QValueKindToClickHouseTypeMap[q.Type]; ok {
			var err error
			colType, err = q.getClickHouseTypeForNumericField(ctx, env)
			if err != nil {
				return "", err
			}
		} else if q.Type == QValueKindNumeric {
			colType = val
		} else {
			colType = "String"
		}
		if nullableEnabled && q.Nullable && !q.Type.IsArray() {
			colType = fmt.Sprintf("Nullable(%s)", colType)
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
	return colType, nil
}
