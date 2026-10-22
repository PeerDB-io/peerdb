package connmongo

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const documentLookupCheckpointPrefix = "mongo-lookup-v1:"

// The read fence belongs to the destination-acknowledged checkpoint, not just the
// live cursor. Otherwise a new connector could fetch an older majority snapshot
// than an image already written before the restart. The embedded resume token is
// unchanged and is the only part sent to MongoDB or used to calculate CDC lag.
func encodeDocumentLookupCheckpoint(token string, fence bson.Timestamp) string {
	if token == "" {
		return ""
	}
	return fmt.Sprintf("%s%d:%d:%s", documentLookupCheckpointPrefix, fence.T, fence.I, token)
}

func decodeMongoCheckpoint(text string) (bson.Raw, bson.Timestamp, bool, error) {
	var fence bson.Timestamp
	wrapped := strings.HasPrefix(text, documentLookupCheckpointPrefix)
	if wrapped {
		fields := strings.SplitN(strings.TrimPrefix(text, documentLookupCheckpointPrefix), ":", 3)
		if len(fields) != 3 || fields[2] == "" {
			return nil, fence, true, fmt.Errorf("invalid MongoDB document lookup checkpoint")
		}
		seconds, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			return nil, fence, true, fmt.Errorf("invalid MongoDB lookup checkpoint timestamp: %w", err)
		}
		increment, err := strconv.ParseUint(fields[1], 10, 32)
		if err != nil {
			return nil, fence, true, fmt.Errorf("invalid MongoDB lookup checkpoint increment: %w", err)
		}
		fence = bson.Timestamp{T: uint32(seconds), I: uint32(increment)}
		text = fields[2]
	}
	token, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, fence, wrapped, err
	}
	if err := bson.Raw(token).Validate(); err != nil {
		return nil, fence, wrapped, fmt.Errorf("invalid MongoDB resume token BSON: %w", err)
	}
	return token, fence, wrapped, nil
}
