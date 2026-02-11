package shared

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"

	"github.com/youmark/pkcs8"
	"golang.org/x/crypto/chacha20poly1305"
)

func DecodePKCS8PrivateKey(rawKey []byte, password *string) (*rsa.PrivateKey, error) {
	PEMBlock, _ := pem.Decode(rawKey)
	if PEMBlock == nil {
		return nil, errors.New("failed to decode private key PEM block")
	}

	var privateKey *rsa.PrivateKey
	var err error
	if password != nil {
		privateKey, err = pkcs8.ParsePKCS8PrivateKeyRSA(PEMBlock.Bytes, []byte(*password))
	} else {
		privateKey, err = pkcs8.ParsePKCS8PrivateKeyRSA(PEMBlock.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key PEM block as PKCS8: %w", err)
	}

	return privateKey, nil
}

// PeerDBEncKey is a key for encrypting and decrypting data.
type PeerDBEncKey struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type PeerDBEncKeys []PeerDBEncKey

func (e PeerDBEncKeys) Get(id string) (PeerDBEncKey, error) {
	if id == "" {
		return PeerDBEncKey{}, nil
	}

	for _, key := range e {
		if key.ID == id {
			return key, nil
		}
	}

	return PeerDBEncKey{}, fmt.Errorf("failed to find encryption key %s", id)
}

const nonceSize = chacha20poly1305.NonceSizeX

// Decrypt decrypts the given ciphertext using the PeerDBEncKey.
func (key PeerDBEncKey) Decrypt(ciphertext []byte) ([]byte, error) {
	if key.ID == "" {
		return ciphertext, nil
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
func (key PeerDBEncKey) Encrypt(plaintext []byte) ([]byte, error) {
	if key.ID == "" {
		return plaintext, nil
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
