package connmysql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestMySQLCloudSQLAuthRejectsUnsafeTLS(t *testing.T) {
	for name, config := range map[string]*protos.MySqlConfig{
		"TLS disabled":           {DisableTls: true},
		"skip cert verification": {SkipCertVerification: true},
		"no root CA or TLS host": {},
	} {
		t.Run(name, func(t *testing.T) {
			config.AuthType = protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH
			_, err := NewMySqlConnector(t.Context(), config)
			_, ok := errors.AsType[*exceptions.CloudSQLIAMAuthError](err)
			require.True(t, ok, "expected CloudSQLIAMAuthError, got %v", err)
		})
	}
}

func TestMySQLCloudSQLTLSIdentityPolicy(t *testing.T) {
	_, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
	})
	require.ErrorContains(t, err, "requires a non-empty root CA")

	rootCA := generateMySQLRootCA(t)
	chainOnlyConfig, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		RootCa:   &rootCA,
	})
	require.NoError(t, err)
	require.True(t, chainOnlyConfig.InsecureSkipVerify)
	require.NotNil(t, chainOnlyConfig.VerifyConnection)
	require.Empty(t, chainOnlyConfig.ServerName)

	tlsHostConfig, err := mySQLTLSConfig(&protos.MySqlConfig{
		Host:     "synthetic-rpe-alias.internal",
		AuthType: protos.MySqlAuthType_MYSQL_GCP_CLOUD_SQL_IAM_AUTH,
		TlsHost:  "cloudsql.google.internal",
	})
	require.NoError(t, err)
	require.False(t, tlsHostConfig.InsecureSkipVerify)
	require.Nil(t, tlsHostConfig.VerifyConnection)
	require.Equal(t, "cloudsql.google.internal", tlsHostConfig.ServerName)
}

func generateMySQLRootCA(t *testing.T) string {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
}
