package utils

import (
	"context"
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
	require.ErrorIs(t, err, ErrSecretNotFound)
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

func TestWaitForTLSCertificateImmediateSuccess(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)
	secret := makeTLSSecret("immediate-secret", map[string][]byte{
		"tls.crt": certs.ClientCertPEM,
		"tls.key": certs.ClientKeyPEM,
		"ca.crt":  certs.CACertPEM,
	})
	store := newTestStore(t, secret)

	cert, caCert, err := store.WaitForTLSCertificate(t.Context(), "immediate-secret")
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.NotEmpty(t, caCert)
}

func TestWaitForTLSCertificateArrivesAfterDelay(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)

	// Create store with no secrets initially
	clientset := fake.NewClientset()
	store, err := newK8sSecretStoreFromClientset(clientset, testNamespace)
	require.NoError(t, err)
	t.Cleanup(store.Close)

	// Override backoff to keep the test fast: 3 short retries
	origBackoff := waitBackoffIntervals
	waitBackoffIntervals = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
	t.Cleanup(func() { waitBackoffIntervals = origBackoff })

	// Add the secret after a short delay (between 1st and 2nd retry)
	go func() {
		time.Sleep(150 * time.Millisecond)
		secret := makeTLSSecret("delayed-secret", map[string][]byte{
			"tls.crt": certs.ClientCertPEM,
			"tls.key": certs.ClientKeyPEM,
			"ca.crt":  certs.CACertPEM,
		})
		_, createErr := clientset.CoreV1().Secrets(testNamespace).Create(
			context.Background(), secret, metav1.CreateOptions{},
		)
		if createErr != nil {
			panic("failed to create secret in test goroutine: " + createErr.Error())
		}
	}()

	cert, caCert, err := store.WaitForTLSCertificate(t.Context(), "delayed-secret")
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.NotEmpty(t, caCert)
}

func TestWaitForTLSCertificateContextCancelled(t *testing.T) {
	t.Parallel()
	store := newTestStore(t) // no secrets — will never find one

	// Override backoff to keep the test fast
	origBackoff := waitBackoffIntervals
	waitBackoffIntervals = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	t.Cleanup(func() { waitBackoffIntervals = origBackoff })

	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()

	cert, caCert, err := store.WaitForTLSCertificate(ctx, "never-arrives")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, cert)
	require.Nil(t, caCert)
}

func TestWaitForTLSCertificateNonRetryableError(t *testing.T) {
	t.Parallel()
	certs := generateTestCerts(t)

	// Secret exists but is missing tls.crt — this is a non-retryable error
	secret := makeTLSSecret("bad-secret", map[string][]byte{
		"tls.key": certs.ClientKeyPEM,
	})
	store := newTestStore(t, secret)

	// Override backoff so we can assert that it does NOT retry
	origBackoff := waitBackoffIntervals
	waitBackoffIntervals = []time.Duration{5 * time.Second}
	t.Cleanup(func() { waitBackoffIntervals = origBackoff })

	start := time.Now()
	cert, caCert, err := store.WaitForTLSCertificate(t.Context(), "bad-secret")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "tls.crt")
	require.NotErrorIs(t, err, ErrSecretNotFound)
	require.Nil(t, cert)
	require.Nil(t, caCert)
	// Should return immediately, not after the 5s backoff
	require.Less(t, elapsed, 1*time.Second)
}
