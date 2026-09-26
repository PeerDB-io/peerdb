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

func TestCreateTlsConfigCertificateChainOnlyVerification(t *testing.T) {
	rootPEM, leaf, intermediate := generateServerCertificateChain(t, "cloudsql.google.internal")

	config, err := CreateTlsConfig(
		tls.VersionTLS12,
		&rootPEM,
		"synthetic-rpe-alias.internal",
		"",
		false,
		nil,
		WithCertificateChainOnlyVerification(),
	)
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
	require.Empty(t, config.ServerName)
	require.NotNil(t, config.VerifyConnection)
	require.NoError(t, config.VerifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf, intermediate},
	}))
}

func TestCreateTlsConfigCertificateChainOnlyVerificationRequiresRootCA(t *testing.T) {
	_, err := CreateTlsConfig(
		tls.VersionTLS12,
		nil,
		"synthetic-rpe-alias.internal",
		"",
		false,
		nil,
		WithCertificateChainOnlyVerification(),
	)
	require.ErrorContains(t, err, "requires a non-empty root CA")
}

func generateServerCertificateChain(
	t *testing.T,
	dnsName string,
) (string, *x509.Certificate, *x509.Certificate) {
	t.Helper()
	now := time.Now()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(10),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	require.NoError(t, err)
	root, err := x509.ParseCertificate(rootDER)
	require.NoError(t, err)

	intermediateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	intermediateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(11),
		Subject:               pkix.Name{CommonName: "test intermediate"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	intermediateDER, err := x509.CreateCertificate(
		rand.Reader,
		intermediateTemplate,
		root,
		&intermediateKey.PublicKey,
		rootKey,
	)
	require.NoError(t, err)
	intermediate, err := x509.ParseCertificate(intermediateDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(12),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(
		rand.Reader,
		leafTemplate,
		intermediate,
		&leafKey.PublicKey,
		intermediateKey,
	)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)

	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	return string(rootPEM), leaf, intermediate
}
