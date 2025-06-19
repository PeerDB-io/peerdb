package connmongo

import (
	"fmt"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func qValueStringFromKey(key interface{}) (types.QValueString, error) {
	var qValue types.QValueString
	switch v := key.(type) {
	case int:
		qValue = types.QValueString{Val: strconv.Itoa(v)}
	case int32:
		qValue = types.QValueString{Val: strconv.Itoa(int(v))}
	case int64:
		qValue = types.QValueString{Val: strconv.FormatInt(v, 10)}
	case string:
		qValue = types.QValueString{Val: v}
	case float32, float64:
		qValue = types.QValueString{Val: fmt.Sprintf("%f", v)}
	case bson.Decimal128:
		qValue = types.QValueString{Val: v.String()}
	case bson.ObjectID:
		qValue = types.QValueString{Val: v.Hex()}
	case bson.Timestamp:
		// https://www.mongodb.com/docs/v7.0/reference/bson-types/#timestamps
		ts := (uint64(v.T) << 32) | uint64(v.I)
		qValue = types.QValueString{Val: strconv.FormatUint(ts, 10)}
	case bson.DateTime:
		qValue = types.QValueString{Val: strconv.FormatInt(int64(v), 10)}
	case bson.Binary:
		qValue = types.QValueString{Val: string(v.Data)}
	case bson.D:
		jsonb, err := bson.MarshalExtJSON(key, true, true)
		if err != nil {
			return types.QValueString{}, fmt.Errorf("error marshalling key '%v': %w", key, err)
		}
		qValue = types.QValueString{Val: string(jsonb)}
	default:
		return types.QValueString{}, fmt.Errorf("unexpected key '%v' with format: %T", key, key)
	}
	if qValue.Val == "" {
		return types.QValueString{}, fmt.Errorf("key can not be empty")
	}
	return qValue, nil
}

func qValueJSONFromDoc(document interface{}) (types.QValueJSON, error) {
	jsonb, err := bson.MarshalExtJSON(document, true, true)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("error marshalling document [%v]: %w", document, err)
	}
	return types.QValueJSON{Val: string(jsonb), IsArray: false}, nil
}
