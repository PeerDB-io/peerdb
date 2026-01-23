package connmongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var API = CreateExtendedJSONMarshaler()

func qValueStringFromKey(key any, version uint32) (types.QValueString, error) {
	// For new mirrors, store ObjectID and string keys without redundant quotes
	if version >= shared.InternalVersion_MongoDBIdWithoutRedundantQuotes {
		switch v := key.(type) {
		case bson.ObjectID:
			return types.QValueString{Val: v.Hex()}, nil
		case string:
			return types.QValueString{Val: v}, nil
		}
	}
	jsonb, err := API.Marshal(key)
	if err != nil {
		return types.QValueString{}, fmt.Errorf("error marshalling key: %w", err)
	}
	return types.QValueString{Val: string(jsonb)}, nil
}

func qValueJSONFromDocument(document any) (types.QValueJSON, error) {
	jsonb, err := API.Marshal(document)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("error marshalling document: %w", err)
	}
	return types.QValueJSON{Val: string(jsonb), IsArray: false}, nil
}
