package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func PostgresOIDToQValueKind(
	recvOID uint32,
	customTypeMapping map[uint32]shared.CustomDataType,
	typeMap *pgtype.Map,
	version uint32,
) (types.QValueKind, error) {
	switch recvOID {
	case pgtype.BoolOID:
		return types.QValueKindBoolean, nil
	case pgtype.Int2OID:
		return types.QValueKindInt16, nil
	case pgtype.Int4OID:
		return types.QValueKindInt32, nil
	case pgtype.Int8OID:
		return types.QValueKindInt64, nil
	case pgtype.Float4OID:
		return types.QValueKindFloat32, nil
	case pgtype.Float8OID:
		return types.QValueKindFloat64, nil
	case pgtype.QCharOID:
		return types.QValueKindQChar, nil
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return types.QValueKindString, nil
	case pgtype.ByteaOID:
		return types.QValueKindBytes, nil
	case pgtype.JSONOID:
		return types.QValueKindJSON, nil
	case pgtype.JSONBOID:
		return types.QValueKindJSONB, nil
	case pgtype.UUIDOID:
		return types.QValueKindUUID, nil
	case pgtype.TimeOID:
		return types.QValueKindTime, nil
	case pgtype.DateOID:
		return types.QValueKindDate, nil
	case pgtype.CIDROID:
		return types.QValueKindCIDR, nil
	case pgtype.MacaddrOID:
		return types.QValueKindMacaddr, nil
	case pgtype.InetOID:
		return types.QValueKindINET, nil
	case pgtype.TimestampOID:
		return types.QValueKindTimestamp, nil
	case pgtype.TimestamptzOID:
		return types.QValueKindTimestampTZ, nil
	case pgtype.NumericOID:
		return types.QValueKindNumeric, nil
	case pgtype.Int2ArrayOID:
		return types.QValueKindArrayInt16, nil
	case pgtype.Int4ArrayOID:
		return types.QValueKindArrayInt32, nil
	case pgtype.Int8ArrayOID:
		return types.QValueKindArrayInt64, nil
	case pgtype.PointOID:
		return types.QValueKindPoint, nil
	case pgtype.Float4ArrayOID:
		return types.QValueKindArrayFloat32, nil
	case pgtype.Float8ArrayOID:
		return types.QValueKindArrayFloat64, nil
	case pgtype.BoolArrayOID:
		return types.QValueKindArrayBoolean, nil
	case pgtype.DateArrayOID:
		return types.QValueKindArrayDate, nil
	case pgtype.TimestampArrayOID:
		return types.QValueKindArrayTimestamp, nil
	case pgtype.TimestamptzArrayOID:
		return types.QValueKindArrayTimestampTZ, nil
	case pgtype.UUIDArrayOID:
		return types.QValueKindArrayUUID, nil
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID:
		return types.QValueKindArrayString, nil
	case pgtype.JSONArrayOID:
		return types.QValueKindArrayJSON, nil
	case pgtype.JSONBArrayOID:
		return types.QValueKindArrayJSONB, nil
	case pgtype.NumericArrayOID:
		return types.QValueKindArrayNumeric, nil
	case pgtype.IntervalOID:
		return types.QValueKindInterval, nil
	case pgtype.IntervalArrayOID:
		return types.QValueKindArrayInterval, nil
	default:
		if typeName, ok := typeMap.TypeForOID(recvOID); ok {
			colType := types.QValueKindString
			if typeData, ok := customTypeMapping[recvOID]; ok {
				colType = CustomTypeToQKind(typeData, version)
			}
			return colType, errors.New(typeName.Name)
		} else {
			// workaround for some types not being defined by pgtype
			switch recvOID {
			case pgtype.TimetzOID:
				return types.QValueKindTimeTZ, nil
			case pgtype.PointOID:
				return types.QValueKindPoint, nil
			default:
				if typeData, ok := customTypeMapping[recvOID]; ok {
					return CustomTypeToQKind(typeData, version), nil
				}
				return types.QValueKindString, nil
			}
		}
	}
}

func CustomTypeToQKind(typeData shared.CustomDataType, version uint32) types.QValueKind {
	if typeData.Type == 'e' {
		if typeData.Delim != 0 {
			return types.QValueKindArrayEnum
		} else {
			return types.QValueKindEnum
		}
	}
	if version >= shared.InternalVersion_CompositeTypeAsTuple {
		if typeData.Type == 'c' {
			if typeData.Delim != 0 {
				return types.QValueKindArrayComposite
			} else {
				return types.QValueKindComposite
			}
		}
	}

	if typeData.Delim != 0 {
		return types.QValueKindArrayString
	}

	switch typeData.Name {
	case "geometry":
		return types.QValueKindGeometry
	case "geography":
		return types.QValueKindGeography
	case "hstore":
		return types.QValueKindHStore
	case "vector", "halfvec", "sparsevec":
		if version >= shared.InternalVersion_PgVectorAsFloatArray {
			return types.QValueKindArrayFloat32
		} else {
			return types.QValueKindString
		}
	default:
		return types.QValueKindString
	}
}
