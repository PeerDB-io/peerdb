package qvalue

import (
	"context"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
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
		// ignore the case of the field name convert to lower case
		f1 := strings.ToLower(field.Name)
		f2 := strings.ToLower(other.Fields[i].Name)
		if f1 != f2 {
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
func (q QField) ToDWHColumnType(ctx context.Context, env map[string]string, dwhType protos.DBType) (string, error) {
	switch dwhType {
	case protos.DBType_SNOWFLAKE:
		if val, ok := QValueKindToSnowflakeTypeMap[q.Type]; ok {
			return val, nil
		} else if q.Type == QValueKindNumeric {
			return fmt.Sprintf("NUMERIC(%d,%d)", q.Precision, q.Scale), nil
		} else {
			return "STRING", nil
		}
	case protos.DBType_CLICKHOUSE:
		if val, ok := QValueKindToClickHouseTypeMap[q.Type]; ok {
			return q.getClickHouseTypeForNumericField(ctx, env)
		} else if q.Type == QValueKindNumeric {
			return val, nil
		} else {
			return "String", nil
		}
	default:
		return "", fmt.Errorf("unknown dwh type: %v", dwhType)
	}
}
