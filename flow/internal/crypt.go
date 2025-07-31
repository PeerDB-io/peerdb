package internal

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func Decrypt(ctx context.Context, encKeyID string, payload []byte) ([]byte, error) {
	if encKeyID == "" {
		return payload, nil
	}

	keys := PeerDBEncKeys(ctx)
	key, err := keys.Get(encKeyID)
	if err != nil {
		return nil, exceptions.NewDecryptError(err)
	}

	if plaintext, err := key.Decrypt(payload); err != nil {
		return nil, exceptions.NewDecryptError(err)
	} else {
		return plaintext, nil
	}
}
