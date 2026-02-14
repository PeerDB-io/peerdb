package utils

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const testNamespace = "test-ns"

type testCertBundle struct {
	CACertPEM     []byte
	ClientCertPEM []byte
	ClientKeyPEM  []byte
}

func generateTestCerts(t *testing.T) testCertBundle {
	t.Helper()

	// Generate CA key and self-signed cert
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate client key and cert signed by CA
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "peerdb-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})

	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER})

	return testCertBundle{
		CACertPEM:     caCertPEM,
		ClientCertPEM: clientCertPEM,
		ClientKeyPEM:  clientKeyPEM,
	}
}

func makeTLSSecret(name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: data,
	}
}

func newTestStore(t *testing.T, secrets ...*corev1.Secret) *K8sSecretStore {
	t.Helper()

	clientset := fake.NewClientset()
	for _, s := range secrets {
		_, err := clientset.CoreV1().Secrets(s.Namespace).Create(
			t.Context(), s, metav1.CreateOptions{},
		)
		require.NoError(t, err)
	}

	store, err := newK8sSecretStoreFromClientset(clientset, testNamespace)
	require.NoError(t, err)
	t.Cleanup(store.Close)

	return store
}

func TestGetTLSCertificateValidSecret(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)
	secret := makeTLSSecret("my-tls-secret", map[string][]byte{
		"tls.crt": certs.ClientCertPEM,
		"tls.key": certs.ClientKeyPEM,
		"ca.crt":  certs.CACertPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.GetTLSCertificate("my-tls-secret")
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.NotEmpty(t, caCert)
	// Verify the parsed certificate has the expected CN
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.Equal(t, "peerdb-client", parsed.Subject.CommonName)
}

func TestGetTLSCertificateNoCACert(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)
	secret := makeTLSSecret("no-ca-secret", map[string][]byte{
		"tls.crt": certs.ClientCertPEM,
		"tls.key": certs.ClientKeyPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.GetTLSCertificate("no-ca-secret")
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Nil(t, caCert)
}

func TestGetTLSCertificateMissingSecret(t *testing.T) {
	t.Parallel()
	store := newTestStore(t) // no secrets

	cert, caCert, err := store.GetTLSCertificate("nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
	require.Nil(t, cert)
	require.Nil(t, caCert)
}

func TestGetTLSCertificateMissingCertKey(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)
	secret := makeTLSSecret("missing-cert", map[string][]byte{
		"tls.key": certs.ClientKeyPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.GetTLSCertificate("missing-cert")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tls.crt")
	require.Nil(t, cert)
	require.Nil(t, caCert)
}

func TestGetTLSCertificateMissingPrivateKey(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)
	secret := makeTLSSecret("missing-key", map[string][]byte{
		"tls.crt": certs.ClientCertPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.GetTLSCertificate("missing-key")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tls.key")
	require.Nil(t, cert)
	require.Nil(t, caCert)
}

func TestGetTLSCertificateInvalidCertKeyPair(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)

	// Generate a second unrelated key
	otherCerts := generateTestCerts(t)

	// Use cert from one bundle and key from another — mismatched
	secret := makeTLSSecret("bad-pair", map[string][]byte{
		"tls.crt": certs.ClientCertPEM,
		"tls.key": otherCerts.ClientKeyPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.GetTLSCertificate("bad-pair")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse TLS certificate")
	require.Nil(t, cert)
	require.Nil(t, caCert)
}
