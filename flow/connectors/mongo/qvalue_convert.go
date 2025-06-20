package connmongo

import (
	"encoding/json"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func qValueStringFromKey(key any) (types.QValueString, error) {
	jsonb, err := json.Marshal(key)
	if err != nil {
		return types.QValueString{}, fmt.Errorf("error marshalling key: %w", err)
	}
	return types.QValueString{Val: string(jsonb)}, nil
}

func qValueJSONFromDocument(document interface{}) (types.QValueJSON, error) {
	jsonb, err := json.Marshal(document)
	if err != nil {
		return types.QValueJSON{}, fmt.Errorf("error marshalling document: %w", err)
	}
	return types.QValueJSON{Val: string(jsonb), IsArray: false}, nil
}
