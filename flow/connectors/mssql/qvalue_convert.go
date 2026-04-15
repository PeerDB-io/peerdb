package connmssql

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func QValueFromMssqlValue(qkind types.QValueKind, val any) (types.QValue, error) {
	if val == nil {
		return types.QValueNull(qkind), nil
	}

	switch qkind {
	case types.QValueKindBoolean:
		if v, ok := val.(bool); ok {
			return types.QValueBoolean{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected bool for %s, got %T", qkind, val)
		}

	case types.QValueKindUInt8:
		if v, ok := val.(int64); ok {
			return types.QValueUInt8{Val: uint8(v)}, nil
		} else {
			return nil, fmt.Errorf("expected int64 for %s, got %T", qkind, val)
		}

	case types.QValueKindInt16:
		if v, ok := val.(int64); ok {
			return types.QValueInt16{Val: int16(v)}, nil
		} else {
			return nil, fmt.Errorf("expected int64 for %s, got %T", qkind, val)
		}

	case types.QValueKindInt32:
		if v, ok := val.(int64); ok {
			return types.QValueInt32{Val: int32(v)}, nil
		} else {
			return nil, fmt.Errorf("expected int64 for %s, got %T", qkind, val)
		}

	case types.QValueKindInt64:
		if v, ok := val.(int64); ok {
			return types.QValueInt64{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected int64 for %s, got %T", qkind, val)
		}

	case types.QValueKindFloat32:
		if v, ok := val.(float64); ok {
			return types.QValueFloat32{Val: float32(v)}, nil
		} else {
			return nil, fmt.Errorf("expected float64 for %s, got %T", qkind, val)
		}

	case types.QValueKindFloat64:
		if v, ok := val.(float64); ok {
			return types.QValueFloat64{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected float64 for %s, got %T", qkind, val)
		}

	case types.QValueKindNumeric:
		switch v := val.(type) {
		case []byte:
			d, err := decimal.NewFromString(string(v))
			if err != nil {
				return nil, fmt.Errorf("failed to parse decimal from %s: %w", string(v), err)
			}
			return types.QValueNumeric{Val: d}, nil
		case string:
			d, err := decimal.NewFromString(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse decimal from %s: %w", v, err)
			}
			return types.QValueNumeric{Val: d}, nil
		case float64:
			return types.QValueNumeric{Val: decimal.NewFromFloat(v)}, nil
		default:
			return nil, fmt.Errorf("expected []byte/string/float64 for %s, got %T", qkind, val)
		}

	case types.QValueKindString:
		switch v := val.(type) {
		case string:
			return types.QValueString{Val: v}, nil
		case []byte:
			return types.QValueString{Val: string(v)}, nil
		default:
			// sql_variant and other polymorphic types can return any Go type
			return types.QValueString{Val: fmt.Sprint(val)}, nil
		}

	case types.QValueKindBytes:
		if v, ok := val.([]byte); ok {
			return types.QValueBytes{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected []byte for %s, got %T", qkind, val)
		}

	case types.QValueKindDate:
		if v, ok := val.(time.Time); ok {
			return types.QValueDate{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected time.Time for %s, got %T", qkind, val)
		}

	case types.QValueKindTime:
		if v, ok := val.(time.Time); ok {
			midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())
			return types.QValueTime{Val: time.Duration(v.UnixNano() - midnight.UnixNano())}, nil
		} else {
			return nil, fmt.Errorf("expected time.Time for %s, got %T", qkind, val)
		}

	case types.QValueKindTimestamp:
		if v, ok := val.(time.Time); ok {
			return types.QValueTimestamp{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected time.Time for %s, got %T", qkind, val)
		}

	case types.QValueKindTimestampTZ:
		if v, ok := val.(time.Time); ok {
			return types.QValueTimestampTZ{Val: v}, nil
		} else {
			return nil, fmt.Errorf("expected time.Time for %s, got %T", qkind, val)
		}

	case types.QValueKindUUID:
		switch v := val.(type) {
		case [16]byte:
			return types.QValueUUID{Val: uuid.UUID(v)}, nil
		case []byte:
			if len(v) == 16 {
				return types.QValueUUID{Val: uuid.UUID(v)}, nil
			}
			parsed, err := uuid.ParseBytes(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse UUID: %w", err)
			}
			return types.QValueUUID{Val: parsed}, nil
		case string:
			parsed, err := uuid.Parse(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse UUID: %w", err)
			}
			return types.QValueUUID{Val: parsed}, nil
		default:
			return nil, fmt.Errorf("expected [16]byte/[]byte/string for %s, got %T", qkind, val)
		}

	case types.QValueKindGeometry:
		switch v := val.(type) {
		case []byte:
			return types.QValueGeometry{Val: string(v)}, nil
		case string:
			return types.QValueGeometry{Val: v}, nil
		default:
			return nil, fmt.Errorf("expected []byte/string for %s, got %T", qkind, val)
		}

	case types.QValueKindGeography:
		switch v := val.(type) {
		case []byte:
			return types.QValueGeography{Val: string(v)}, nil
		case string:
			return types.QValueGeography{Val: v}, nil
		default:
			return nil, fmt.Errorf("expected []byte/string for %s, got %T", qkind, val)
		}

	default:
		return types.QValueInvalid{Val: fmt.Sprint(val)}, nil
	}
}
