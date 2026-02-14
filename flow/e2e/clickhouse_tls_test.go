package e2e

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func readTLSEnvPaths(t *testing.T) ([]byte, []byte, []byte) {
	t.Helper()

	caPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CA_CERT_PATH")
	certPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CLIENT_CERT_PATH")
	keyPath := os.Getenv("PEERDB_CLICKHOUSE_TLS_CLIENT_KEY_PATH")

	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, nil, nil
	}

	caCert, err := os.ReadFile(caPath)
	require.NoError(t, err, "failed to read CA cert from %s", caPath)
	clientCert, err := os.ReadFile(certPath)
	require.NoError(t, err, "failed to read client cert from %s", certPath)
	clientKey, err := os.ReadFile(keyPath)
	require.NoError(t, err, "failed to read client key from %s", keyPath)

	return caCert, clientCert, clientKey
}

func tlsClickHousePort(t *testing.T) uint32 {
	t.Helper()
	portStr := os.Getenv("PEERDB_CLICKHOUSE_TLS_PORT")
	if portStr == "" {
		return 0
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(t, err, "invalid PEERDB_CLICKHOUSE_TLS_PORT value: %s", portStr)
	return uint32(port)
}

// TestClickHouseTLS_InlineCerts verifies that connclickhouse.Connect works
// with inline PEM certificates against a TLS-enabled ClickHouse instance.
func TestClickHouseTLSInlineCerts(t *testing.T) {
	port := tlsClickHousePort(t)
	if port == 0 {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}

	caCert, clientCert, clientKey := readTLSEnvPaths(t)
	if caCert == nil {
		t.Skip("TLS certificate paths not set, skipping TLS test")
	}

	caCertStr := string(caCert)
	clientCertStr := string(clientCert)
	clientKeyStr := string(clientKey)

	config := &protos.ClickhouseConfig{
		Host:        "localhost",
		Port:        port,
		User:        "peerdb_tls",
		Database:    "default",
		DisableTls:  false,
		Certificate: &clientCertStr,
		PrivateKey:  &clientKeyStr,
		RootCa:      &caCertStr,
	}

	ctx := context.Background()
	conn, err := connclickhouse.Connect(ctx, nil, config)
	require.NoError(t, err, "failed to connect to ClickHouse with inline TLS certs")
	defer conn.Close()

	var result uint64
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "failed to execute SELECT 1")
	require.Equal(t, uint64(1), result)
}

// TestClickHouseTLS_K8sSecret verifies that connclickhouse.Connect works
// with TLS certificates loaded from a (fake) Kubernetes Secret.
func TestClickHouseTLSK8sSecret(t *testing.T) {
	port := tlsClickHousePort(t)
	if port == 0 {
		t.Skip("PEERDB_CLICKHOUSE_TLS_PORT not set, skipping TLS test")
	}

	caCert, clientCert, clientKey := readTLSEnvPaths(t)
	if caCert == nil {
		t.Skip("TLS certificate paths not set, skipping TLS test")
	}

	const (
		testNamespace  = "test-namespace"
		testSecretName = "test-ch-tls-secret" //nolint:gosec // not a credential
	)

	// Create a fake K8s clientset with our TLS secret
	fakeClientset := fake.NewClientset()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSecretName,
			Namespace: testNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"tls.crt": clientCert,
			"tls.key": clientKey,
			"ca.crt":  caCert,
		},
	}
	_, err := fakeClientset.CoreV1().Secrets(testNamespace).Create(
		context.Background(), secret, metav1.CreateOptions{},
	)
	require.NoError(t, err, "failed to create fake K8s secret")

	// Override the K8s clientset resolver and reset the singleton
	originalResolver := utils.DefaultResolveKubernetesClientset
	utils.DefaultResolveKubernetesClientset = func() (kubernetes.Interface, error) {
		return fakeClientset, nil
	}
	t.Cleanup(func() {
		utils.DefaultResolveKubernetesClientset = originalResolver
		utils.ResetK8sSecretStoreForTest()
	})
	utils.ResetK8sSecretStoreForTest()

	// Set POD_NAMESPACE for the secret store
	t.Setenv("POD_NAMESPACE", testNamespace)

	config := &protos.ClickhouseConfig{
		Host:                     "localhost",
		Port:                     port,
		User:                     "peerdb_tls",
		Database:                 "default",
		DisableTls:               false,
		TlsCertificateSecretName: ptr.To(testSecretName),
	}

	env := map[string]string{
		"PEERDB_CLICKHOUSE_TLS_K8S_SECRET_ENABLED": "true",
	}

	ctx := context.Background()
	conn, err := connclickhouse.Connect(ctx, env, config)
	require.NoError(t, err, "failed to connect to ClickHouse with K8s secret TLS certs")
	defer conn.Close()

	var result uint64
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "failed to execute SELECT 1")
	require.Equal(t, uint64(1), result)
}
