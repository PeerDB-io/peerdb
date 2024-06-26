package shared

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// PeerDBEncKey is a key for encrypting and decrypting data.
type PeerDBEncKey struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type PeerDBEncKeys []PeerDBEncKey

func (e *PeerDBEncKeys) Get(id string) (*PeerDBEncKey, error) {
	if id == "" {
		return nil, nil
	}

	for _, key := range *e {
		if key.ID == id {
			return &key, nil
		}
	}

	return nil, fmt.Errorf("failed to find encryption key - %s", id)
}

// Decrypt decrypts the given ciphertext using the PeerDBEncKey.
func (key *PeerDBEncKey) Decrypt(ciphertext []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("encryption key is nil")
	}

	decodedKey, err := base64.StdEncoding.DecodeString(key.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}

	block, err := aes.NewCipher(decodedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create new cipher: %w", err)
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, errors.New("ciphertext is not a multiple of the block size")
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(ciphertext, ciphertext)

	return PKCS7Unpad(ciphertext, aes.BlockSize)
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

	block, err := aes.NewCipher(decodedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create new cipher: %w", err)
	}

	plaintext, err = PKCS7Pad(plaintext, aes.BlockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to pad plaintext: %w", err)
	}

	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], plaintext)

	return ciphertext, nil
}

// PKCS7Pad pads the given data to the specified block length using the PKCS7 padding scheme.
func PKCS7Pad(data []byte, blocklen int) ([]byte, error) {
	padding := blocklen - len(data)%blocklen
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padtext...), nil
}

// PKCS7Unpad removes the PKCS7 padding from the given data.
func PKCS7Unpad(data []byte, blocklen int) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("invalid padding size")
	}

	if len(data)%blocklen != 0 {
		return nil, errors.New("invalid padding on data")
	}

	paddingLen := int(data[len(data)-1])
	if paddingLen > blocklen || paddingLen == 0 {
		return nil, errors.New("invalid padding size")
	}

	for _, v := range data[len(data)-paddingLen:] {
		if int(v) != paddingLen {
			return nil, errors.New("invalid padding")
		}
	}

	return data[:len(data)-paddingLen], nil
}
