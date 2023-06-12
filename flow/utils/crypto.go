package util

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

func DecodePKCS8PrivateKey(rawKey []byte) (*rsa.PrivateKey, error) {
	PEMBlock, _ := pem.Decode(rawKey)
	if PEMBlock == nil {
		return nil, fmt.Errorf("failed to decode private key PEM block")
	}
	privateKeyAny, err := x509.ParsePKCS8PrivateKey(PEMBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key PEM block as PKCS8: %w", err)
	}
	privateKeyRSA, ok := privateKeyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key does not appear to RSA as expected")
	}

	return privateKeyRSA, nil
}
