package connpostgres

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const testTLSConnString = "postgres://user:password@localhost:5432/testdb?sslmode=require"

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

func TestParseConfigClientTLS(t *testing.T) {
	certPEM, keyPEM := generateClientCertKey(t, "peerdb-client")

	t.Run("client cert populates tls.Config.Certificates", func(t *testing.T) {
		connConfig, err := ParseConfig(testTLSConnString, &protos.PostgresConfig{
			Host:       "localhost",
			Port:       5432,
			User:       "user",
			Database:   "testdb",
			RequireTls: true,
			ClientTls: &protos.ClientTlsConfig{
				Certificate: certPEM,
				PrivateKey:  keyPEM,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, connConfig.TLSConfig)
		require.Len(t, connConfig.TLSConfig.Certificates, 1)

		leaf, err := x509.ParseCertificate(connConfig.TLSConfig.Certificates[0].Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "peerdb-client", leaf.Subject.CommonName)
	})

	t.Run("client cert is not applied when TLS is not enabled", func(t *testing.T) {
		connConfig, err := ParseConfig("postgres://user:password@localhost:5432/testdb", &protos.PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "user",
			Database: "testdb",
			ClientTls: &protos.ClientTlsConfig{
				Certificate: certPEM,
				PrivateKey:  keyPEM,
			},
		})
		require.NoError(t, err)
		require.Empty(t, connConfig.TLSConfig.Certificates)
	})

	t.Run("no client cert leaves Certificates empty", func(t *testing.T) {
		connConfig, err := ParseConfig(testTLSConnString, &protos.PostgresConfig{
			Host:       "localhost",
			Port:       5432,
			User:       "user",
			Database:   "testdb",
			RequireTls: true,
		})
		require.NoError(t, err)
		require.NotNil(t, connConfig.TLSConfig)
		require.Empty(t, connConfig.TLSConfig.Certificates)
	})

	t.Run("invalid client cert/key pair is rejected", func(t *testing.T) {
		_, err := ParseConfig(testTLSConnString, &protos.PostgresConfig{
			Host:       "localhost",
			Port:       5432,
			User:       "user",
			Database:   "testdb",
			RequireTls: true,
			ClientTls: &protos.ClientTlsConfig{
				Certificate: certPEM,
				PrivateKey:  "not a valid key",
			},
		})
		require.Error(t, err)
	})

	t.Run("client cert without private key is rejected", func(t *testing.T) {
		_, err := ParseConfig(testTLSConnString, &protos.PostgresConfig{
			Host:       "localhost",
			Port:       5432,
			User:       "user",
			Database:   "testdb",
			RequireTls: true,
			ClientTls: &protos.ClientTlsConfig{
				Certificate: certPEM,
			},
		})
		require.Error(t, err)
	})
}
