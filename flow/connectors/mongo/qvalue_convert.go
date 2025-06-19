package connmongo

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func qValueStringFromKey(key any) (types.QValueString, error) {
	var val string
	switch v := key.(type) {
	case int:
		val = strconv.Itoa(v)
	case int32:
		val = strconv.Itoa(int(v))
	case int64:
		val = strconv.FormatInt(v, 10)
	case string:
		val = v
	case float32:
		val = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		val = strconv.FormatFloat(v, 'f', -1, 64)
	case bson.Decimal128:
		val = v.String()
	case bson.ObjectID:
		val = v.Hex()
	case bson.Timestamp:
		// https://www.mongodb.com/docs/v7.0/reference/bson-types/#timestamps
		ts := (uint64(v.T) << 32) | uint64(v.I)
		val = strconv.FormatUint(ts, 10)
	case bson.DateTime:
		val = strconv.FormatInt(int64(v), 10)
	case bson.Binary:
		val = base64.StdEncoding.EncodeToString(v.Data)
	case bson.D:
		jsonb, err := bson.MarshalExtJSON(key, true, true)
		if err != nil {
			return types.QValueString{}, fmt.Errorf("error marshalling key '%v': %w", key, err)
		}
		val = string(jsonb)
	default:
		return types.QValueString{}, fmt.Errorf("unexpected key '%v' with format: %T", key, key)
	}
	return types.QValueString{Val: val}, nil
}

func qValueJSONFromDocument(document bson.D) (types.QValueJSON, error) {
	jsonb, err := bson.MarshalExtJSON(document, true, true)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("error marshalling document: %w", err)
	}
	return types.QValueJSON{Val: string(jsonb), IsArray: false}, nil
}
