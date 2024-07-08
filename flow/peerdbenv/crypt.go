package peerdbenv

import "fmt"

func Decrypt(encKeyID string, payload []byte) ([]byte, error) {
	if encKeyID == "" {
		return payload, nil
	}

	keys := PeerDBEncKeys()
	key, err := keys.Get(encKeyID)
	if err != nil {
		return nil, fmt.Errorf("failed to load peer, unable to find encryption key - %s", encKeyID)
	}

	return key.Decrypt(payload)
}
