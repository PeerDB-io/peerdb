package types

import (
	pkgtypes "github.com/PeerDB-io/peerdb/flow/pkg/types"
)

// QValueKind and its constants live in flow/pkg/types so that code in the
// flow/pkg module can use them; the aliases below keep existing importers of
// this package working.
type QValueKind = pkgtypes.QValueKind

const (
	QValueKindInvalid     = pkgtypes.QValueKindInvalid
	QValueKindFloat32     = pkgtypes.QValueKindFloat32
	QValueKindFloat64     = pkgtypes.QValueKindFloat64
	QValueKindInt8        = pkgtypes.QValueKindInt8
	QValueKindInt16       = pkgtypes.QValueKindInt16
	QValueKindInt32       = pkgtypes.QValueKindInt32
	QValueKindInt64       = pkgtypes.QValueKindInt64
	QValueKindInt256      = pkgtypes.QValueKindInt256
	QValueKindUInt8       = pkgtypes.QValueKindUInt8
	QValueKindUInt16      = pkgtypes.QValueKindUInt16
	QValueKindUInt32      = pkgtypes.QValueKindUInt32
	QValueKindUInt64      = pkgtypes.QValueKindUInt64
	QValueKindUInt256     = pkgtypes.QValueKindUInt256
	QValueKindBoolean     = pkgtypes.QValueKindBoolean
	QValueKindQChar       = pkgtypes.QValueKindQChar
	QValueKindString      = pkgtypes.QValueKindString
	QValueKindEnum        = pkgtypes.QValueKindEnum
	QValueKindUint16Enum  = pkgtypes.QValueKindUint16Enum
	QValueKindUint64Set   = pkgtypes.QValueKindUint64Set
	QValueKindTimestamp   = pkgtypes.QValueKindTimestamp
	QValueKindTimestampTZ = pkgtypes.QValueKindTimestampTZ
	QValueKindDate        = pkgtypes.QValueKindDate
	QValueKindTime        = pkgtypes.QValueKindTime
	QValueKindTimeTZ      = pkgtypes.QValueKindTimeTZ
	QValueKindInterval    = pkgtypes.QValueKindInterval
	QValueKindNumeric     = pkgtypes.QValueKindNumeric
	QValueKindBytes       = pkgtypes.QValueKindBytes
	QValueKindUUID        = pkgtypes.QValueKindUUID
	QValueKindJSON        = pkgtypes.QValueKindJSON
	QValueKindJSONB       = pkgtypes.QValueKindJSONB
	QValueKindHStore      = pkgtypes.QValueKindHStore
	QValueKindGeography   = pkgtypes.QValueKindGeography
	QValueKindGeometry    = pkgtypes.QValueKindGeometry
	QValueKindPoint       = pkgtypes.QValueKindPoint

	// network types
	QValueKindCIDR    = pkgtypes.QValueKindCIDR
	QValueKindINET    = pkgtypes.QValueKindINET
	QValueKindMacaddr = pkgtypes.QValueKindMacaddr

	// array types
	QValueKindArrayFloat32     = pkgtypes.QValueKindArrayFloat32
	QValueKindArrayFloat64     = pkgtypes.QValueKindArrayFloat64
	QValueKindArrayInt16       = pkgtypes.QValueKindArrayInt16
	QValueKindArrayInt32       = pkgtypes.QValueKindArrayInt32
	QValueKindArrayInt64       = pkgtypes.QValueKindArrayInt64
	QValueKindArrayString      = pkgtypes.QValueKindArrayString
	QValueKindArrayEnum        = pkgtypes.QValueKindArrayEnum
	QValueKindArrayDate        = pkgtypes.QValueKindArrayDate
	QValueKindArrayInterval    = pkgtypes.QValueKindArrayInterval
	QValueKindArrayTimestamp   = pkgtypes.QValueKindArrayTimestamp
	QValueKindArrayTimestampTZ = pkgtypes.QValueKindArrayTimestampTZ
	QValueKindArrayBoolean     = pkgtypes.QValueKindArrayBoolean
	QValueKindArrayJSON        = pkgtypes.QValueKindArrayJSON
	QValueKindArrayJSONB       = pkgtypes.QValueKindArrayJSONB
	QValueKindArrayUUID        = pkgtypes.QValueKindArrayUUID
	QValueKindArrayNumeric     = pkgtypes.QValueKindArrayNumeric
)

var (
	QValueKindToSnowflakeTypeMap  = pkgtypes.QValueKindToSnowflakeTypeMap
	QValueKindToClickHouseTypeMap = pkgtypes.QValueKindToClickHouseTypeMap
)
