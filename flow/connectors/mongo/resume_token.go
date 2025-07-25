package connmongo

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// https://github.com/mongodb/mongo/blob/ebe22809e0641182af43c9e3d00b1e1d6fcb52cc/src/mongo/db/storage/key_string/key_string.cpp#L91
const kTimestamp = 130

// ResumeToken always contains a "_data" field
// ref: https://github.com/mongodb/mongo/blob/ebe22809e0641182af43c9e3d00b1e1d6fcb52cc/src/mongo/db/pipeline/resume_token.h#L134-L151
func decodeTimestampFromResumeToken(resumeToken bson.Raw) (bson.Timestamp, error) {
	var tokenDoc struct {
		Data string `bson:"_data"`
	}

	err := bson.Unmarshal(resumeToken, &tokenDoc)
	if err != nil {
		return bson.Timestamp{}, err
	}

	if tokenDoc.Data == "" {
		return bson.Timestamp{}, errors.New("missing _data field")
	}

	return decodeTimestampFromKeyString(tokenDoc.Data)
}

// ResumeToken's _data field is an encoded hex string. It always starts with a 'timestamp', representing clusterTime
// ref: https://github.com/mongodb/mongo/blob/ebe22809e0641182af43c9e3d00b1e1d6fcb52cc/src/mongo/db/pipeline/resume_token.cpp#L136-L192
func decodeTimestampFromKeyString(hexData string) (bson.Timestamp, error) {
	// decode the hex string to bytes
	keyStringBytes, err := hex.DecodeString(hexData)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid hex string in _data field: %w", err)
	}

	// the resume token should be at least 9 bytes:
	// - first byte encodes kTimestamp
	// - next 8 bytes encode the 64-bit timestamp value in big-endian order
	if len(keyStringBytes) < 9 {
		return bson.Timestamp{}, errors.New("KeyString data too short for timestamp")
	}
	typeByte := keyStringBytes[0]
	if int32(typeByte) != kTimestamp {
		return bson.Timestamp{}, fmt.Errorf("invalid type expecting %d, got %d", kTimestamp, typeByte)
	}
	timestampBytes := keyStringBytes[1:9]
	timestampValue := binary.BigEndian.Uint64(timestampBytes)

	// high 32 bits = seconds, low 32 bits = increment
	// https://www.mongodb.com/docs/manual/reference/bson-types/#timestamps
	epochSeconds := uint32(timestampValue >> 32)
	increment := uint32(timestampValue & 0xFFFFFFFF)

	return bson.Timestamp{
		T: epochSeconds,
		I: increment,
	}, nil
}
