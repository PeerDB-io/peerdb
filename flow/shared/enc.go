package shared

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

// PeerDBEncKey is a key for encrypting and decrypting data.
type PeerDBEncKey struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type PeerDBEncKeys []PeerDBEncKey

func (e PeerDBEncKeys) Get(id string) (*PeerDBEncKey, error) {
	if id == "" {
		return nil, nil
	}

	for _, key := range e {
		if key.ID == id {
			return &key, nil
		}
	}

	return nil, fmt.Errorf("failed to find encryption key - %s", id)
}

const nonceSize = chacha20poly1305.NonceSizeX

// Decrypt decrypts the given ciphertext using the PeerDBEncKey.
func (key *PeerDBEncKey) Decrypt(ciphertext []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("encryption key is nil")
	}

	decodedKey, err := base64.StdEncoding.DecodeString(key.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}

	if len(decodedKey) != 32 {
		return nil, errors.New("invalid key length, must be 32 bytes")
	}

	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce := ciphertext[:nonceSize]
	ciphertext = ciphertext[nonceSize:]

	aead, err := chacha20poly1305.NewX(decodedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChaCha20-Poly1305: %w", err)
	}

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// Encrypt encrypts the given plaintext using the PeerDBEncKey.
func (key *PeerDBEncKey) Encrypt(plaintext []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("encryption key is nil")
	}

	decodedKey, err := base64.StdEncoding.DecodeString(key.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}

	if len(decodedKey) != 32 {
		return nil, errors.New("invalid key length, must be 32 bytes")
	}

	aead, err := chacha20poly1305.NewX(decodedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChaCha20-Poly1305: %w", err)
	}

	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}
