package conncockroachdb

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestGetCRDBConnectionString(t *testing.T) {
	config := &protos.CockroachDBConfig{
		Host:     "localhost",
		Port:     26257,
		User:     "root",
		Password: "",
		Database: "defaultdb",
	}
	// TLS is on unless explicitly disabled
	assert.Equal(t,
		"postgres://root@localhost:26257/defaultdb?application_name=peerdb&client_encoding=UTF8&sslmode=require",
		GetCRDBConnectionString(config, ""))

	config.DisableTls = true
	assert.Equal(t,
		"postgres://root@localhost:26257/defaultdb?application_name=peerdb&client_encoding=UTF8&sslmode=disable",
		GetCRDBConnectionString(config, ""))

	config.DisableTls = false
	config.Password = "p@ss/word"
	assert.Equal(t,
		"postgres://root@localhost:26257/defaultdb?application_name=peerdb_myflow&client_encoding=UTF8&sslmode=require",
		GetCRDBConnectionString(config, "myflow"))

	// the password never appears in the connection string, it is set on the
	// parsed config by ParseConfig instead
	assert.NotContains(t, GetCRDBConnectionString(config, "myflow"), "word")

	// special characters in user and host are escaped by url.URL
	config.User = "user@corp"
	assert.Equal(t,
		"postgres://user%40corp@localhost:26257/defaultdb?application_name=peerdb&client_encoding=UTF8&sslmode=require",
		GetCRDBConnectionString(config, ""))
}

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

func TestParseConfigTLS(t *testing.T) {
	certPEM, keyPEM := generateClientCertKey(t, "peerdb-crdb-client")
	baseConfig := func() *protos.CockroachDBConfig {
		return &protos.CockroachDBConfig{
			Host:     "localhost",
			Port:     26257,
			User:     "root",
			Database: "defaultdb",
		}
	}
	connString := GetCRDBConnectionString(baseConfig(), "")

	t.Run("password is applied to the parsed config, not the conn string", func(t *testing.T) {
		config := baseConfig()
		config.Password = "sekrit"
		connConfig, err := ParseConfig(connString, config)
		require.NoError(t, err)
		require.Equal(t, "sekrit", connConfig.Password)
	})

	t.Run("skip_cert_verification sets InsecureSkipVerify", func(t *testing.T) {
		config := baseConfig()
		config.SkipCertVerification = true
		connConfig, err := ParseConfig(connString, config)
		require.NoError(t, err)
		require.NotNil(t, connConfig.TLSConfig)
		require.True(t, connConfig.TLSConfig.InsecureSkipVerify)
	})

	t.Run("disable_tls leaves the TLS config empty", func(t *testing.T) {
		config := baseConfig()
		config.DisableTls = true
		connConfig, err := ParseConfig(GetCRDBConnectionString(config, ""), config)
		require.NoError(t, err)
		require.Nil(t, connConfig.TLSConfig)
	})

	t.Run("wire protocol is pinned to 3.0", func(t *testing.T) {
		connConfig, err := ParseConfig(connString, baseConfig())
		require.NoError(t, err)
		require.Equal(t, "3.0", connConfig.Config.MaxProtocolVersion)
	})

	t.Run("cert verification is on by default", func(t *testing.T) {
		connConfig, err := ParseConfig(connString, baseConfig())
		require.NoError(t, err)
		require.NotNil(t, connConfig.TLSConfig)
		require.False(t, connConfig.TLSConfig.InsecureSkipVerify)
		require.Empty(t, connConfig.TLSConfig.Certificates)
	})

	t.Run("client_tls populates tls.Config.Certificates", func(t *testing.T) {
		config := baseConfig()
		config.ClientTls = &protos.ClientTlsConfig{
			Certificate: certPEM,
			PrivateKey:  keyPEM,
		}
		connConfig, err := ParseConfig(connString, config)
		require.NoError(t, err)
		require.NotNil(t, connConfig.TLSConfig)
		require.Len(t, connConfig.TLSConfig.Certificates, 1)

		leaf, err := x509.ParseCertificate(connConfig.TLSConfig.Certificates[0].Certificate[0])
		require.NoError(t, err)
		require.Equal(t, "peerdb-crdb-client", leaf.Subject.CommonName)
	})

	t.Run("client_tls without private key is rejected", func(t *testing.T) {
		config := baseConfig()
		config.ClientTls = &protos.ClientTlsConfig{Certificate: certPEM}
		_, err := ParseConfig(connString, config)
		require.Error(t, err)
	})
}

func TestClassifyConnectError(t *testing.T) {
	for _, code := range []string{
		pgerrcode.InvalidAuthorizationSpecification,
		pgerrcode.InvalidPassword,
		pgerrcode.InsufficientPrivilege,
	} {
		err := classifyConnectError(fmt.Errorf("failed to connect: %w",
			&pgconn.PgError{Code: code, Message: "password authentication failed for user root"}))
		var authErr *exceptions.AuthError
		require.ErrorAs(t, err, &authErr, "SQLSTATE %s should classify as AuthError", code)
	}

	// other pg errors and non-pg errors pass through unchanged
	serialization := fmt.Errorf("connect: %w", &pgconn.PgError{Code: pgerrcode.SerializationFailure})
	require.Equal(t, serialization, classifyConnectError(serialization))
	network := errors.New("dial tcp: connection refused")
	require.Equal(t, network, classifyConnectError(network))
	require.NoError(t, classifyConnectError(nil))
}

func TestParseCrdbMajorVersion(t *testing.T) {
	major, err := parseCrdbMajorVersion("CockroachDB CCL v25.4.13 (x86_64-pc-linux-gnu, built 2026/07/10)")
	require.NoError(t, err)
	assert.Equal(t, 25, major)

	major, err = parseCrdbMajorVersion("CockroachDB v23.1.0 (aarch64-unknown-linux-gnu)")
	require.NoError(t, err)
	assert.Equal(t, 23, major)

	_, err = parseCrdbMajorVersion("PostgreSQL 16.2 on x86_64-pc-linux-gnu")
	require.Error(t, err)
}
