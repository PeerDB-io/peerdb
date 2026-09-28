package common

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// generateClientCertKey returns a self-signed client certificate and its private key, PEM-encoded.
func generateClientCertKey(t *testing.T, commonName string) (string, string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return string(certPEM), string(keyPEM)
}

func TestCreateTlsConfigClientCertificate(t *testing.T) {
	certPEM, keyPEM := generateClientCertKey(t, "peerdb-client")
	otherCertPEM, _ := generateClientCertKey(t, "other-client")

	t.Run("valid certificate and key are loaded", func(t *testing.T) {
		clientCert, err := NewClientCertificate(certPEM, keyPEM)
		require.NoError(t, err)

		config, err := CreateTlsConfig(tls.VersionTLS12, nil, "localhost", "", false, clientCert)
		require.NoError(t, err)
		require.Len(t, config.Certificates, 1)

		leaf, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "peerdb-client", leaf.Subject.CommonName)
	})

	t.Run("no client certificate leaves Certificates empty", func(t *testing.T) {
		config, err := CreateTlsConfig(tls.VersionTLS12, nil, "localhost", "", false, nil)
		require.NoError(t, err)
		require.Empty(t, config.Certificates)
	})

	t.Run("NewClientCertificate rejects a missing certificate", func(t *testing.T) {
		clientCert, err := NewClientCertificate("", keyPEM)
		require.Error(t, err)
		require.Nil(t, clientCert)
	})

	t.Run("NewClientCertificate rejects a missing private key", func(t *testing.T) {
		clientCert, err := NewClientCertificate(certPEM, "")
		require.Error(t, err)
		require.Nil(t, clientCert)
	})

	t.Run("malformed certificate is rejected", func(t *testing.T) {
		clientCert, err := NewClientCertificate("not a pem", keyPEM)
		require.NoError(t, err)
		_, err = CreateTlsConfig(tls.VersionTLS12, nil, "localhost", "", false, clientCert)
		require.Error(t, err)
	})

	t.Run("certificate not matching key is rejected", func(t *testing.T) {
		clientCert, err := NewClientCertificate(otherCertPEM, keyPEM)
		require.NoError(t, err)
		_, err = CreateTlsConfig(tls.VersionTLS12, nil, "localhost", "", false, clientCert)
		require.Error(t, err)
	})
}
