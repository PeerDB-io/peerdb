package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestEngineToStringExhaustive(t *testing.T) {
	for enumVal := range protos.TableEngine_name {
		_, err := engineToString(protos.TableEngine(enumVal))
		assert.NoErrorf(t, err, "engineToString(%v) failed", protos.TableEngine(enumVal))
	}
}
