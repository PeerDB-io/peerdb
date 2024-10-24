package peerdbenv

import (
	"context"
)

func Decrypt(ctx context.Context, encKeyID string, payload []byte) ([]byte, error) {
	if encKeyID == "" {
		return payload, nil
	}

	keys := PeerDBEncKeys(ctx)
	key, err := keys.Get(encKeyID)
	if err != nil {
		return nil, err
	}

	return key.Decrypt(payload)
}
