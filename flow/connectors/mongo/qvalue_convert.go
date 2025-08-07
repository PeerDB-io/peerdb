package connmongo

import (
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

var API = CreateExtendedJSONMarshaler()

func qValueStringFromKey(key any) (types.QValueString, error) {
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
