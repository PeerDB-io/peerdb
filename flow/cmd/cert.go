package main

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"strings"
)

func ProcessCertAndKey(cert string, key string) ([]tls.Certificate, error) {
	temporalCert := strings.TrimSpace(cert)
	certBytes, err := base64.StdEncoding.DecodeString(temporalCert)
	if err != nil {
		return nil, fmt.Errorf("unable to decode temporal certificate: %w", err)
	}

	temporalKey := strings.TrimSpace(key)
	keyBytes, err := base64.StdEncoding.DecodeString(temporalKey)
	if err != nil {
		return nil, fmt.Errorf("unable to decode temporal key: %w", err)
	}

	keyPair, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain temporal key pair: %w", err)
	}

	return []tls.Certificate{keyPair}, nil
}
