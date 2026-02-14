package utils

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	// Standard cert-manager Secret keys
	tlsCertKey   = "tls.crt"
	tlsKeyKey    = "tls.key"
	tlsCACertKey = "ca.crt"
	resyncPeriod = 5 * time.Minute
	namespaceEnv = "POD_NAMESPACE"
)

// K8sSecretStore watches Kubernetes Secrets via informers and provides
// TLS certificate retrieval for ClickHouse peers using cert-manager.
type K8sSecretStore struct {
	clientset kubernetes.Interface
	informer  cache.SharedIndexInformer
	stopCh    chan struct{}
	namespace string
}

// DefaultResolveKubernetesClientset resolves a Kubernetes clientset.
// Tests can override this variable to inject a fake clientset.
var DefaultResolveKubernetesClientset = func() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("not running in Kubernetes cluster: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return clientset, nil
}

var (
	globalSecretStore     *K8sSecretStore
	globalSecretStoreOnce sync.Once
	errGlobalSecretStore  error
)

// GetK8sSecretStore returns the singleton K8sSecretStore instance.
// It initializes on first call using in-cluster config.
// Returns an error if not running inside Kubernetes.
func GetK8sSecretStore() (*K8sSecretStore, error) {
	globalSecretStoreOnce.Do(func() {
		globalSecretStore, errGlobalSecretStore = newK8sSecretStore()
	})
	return globalSecretStore, errGlobalSecretStore
}

// ResetK8sSecretStoreForTest resets the singleton so the next call to
// GetK8sSecretStore will re-initialize. Only for use in tests.
func ResetK8sSecretStoreForTest() {
	if globalSecretStore != nil {
		globalSecretStore.Close()
	}
	globalSecretStore = nil
	errGlobalSecretStore = nil
	globalSecretStoreOnce = sync.Once{}
}

func newK8sSecretStore() (*K8sSecretStore, error) {
	clientset, err := DefaultResolveKubernetesClientset()
	if err != nil {
		return nil, err
	}

	namespace := os.Getenv(namespaceEnv)
	if namespace == "" {
		return nil, fmt.Errorf("environment variable %s is not set", namespaceEnv)
	}

	return newK8sSecretStoreFromClientset(clientset, namespace)
}

// newK8sSecretStoreFromClientset creates a K8sSecretStore with an injected
// clientset and namespace. Used by newK8sSecretStore and tests.
func newK8sSecretStoreFromClientset(clientset kubernetes.Interface, namespace string) (*K8sSecretStore, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		resyncPeriod,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = "type=" + string(corev1.SecretTypeTLS)
		}),
	)

	informer := factory.Core().V1().Secrets().Informer()
	stopCh := make(chan struct{})

	go informer.Run(stopCh)

	// Wait for initial cache sync
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		close(stopCh)
		return nil, errors.New("timed out waiting for Kubernetes Secret informer cache sync")
	}

	slog.InfoContext(ctx, "K8s Secret store initialized", slog.String("namespace", namespace))

	return &K8sSecretStore{
		clientset: clientset,
		namespace: namespace,
		informer:  informer,
		stopCh:    stopCh,
	}, nil
}

// GetTLSCertificate retrieves a parsed TLS certificate from a Kubernetes Secret.
// It reads tls.crt and tls.key from the Secret and parses them into a tls.Certificate.
// Returns the certificate and the raw CA bytes (from ca.crt, may be nil).
func (s *K8sSecretStore) GetTLSCertificate(secretName string) (*tls.Certificate, []byte, error) {
	key := s.namespace + "/" + secretName
	obj, exists, err := s.informer.GetStore().GetByKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("error looking up Secret %q: %w", secretName, err)
	}
	if !exists {
		return nil, nil, fmt.Errorf("secret %q not found in namespace %q", secretName, s.namespace)
	}

	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected object type in informer cache for Secret %q", secretName)
	}

	certPEM, ok := secret.Data[tlsCertKey]
	if !ok || len(certPEM) == 0 {
		return nil, nil, fmt.Errorf("secret %q missing %q key", secretName, tlsCertKey)
	}

	keyPEM, ok := secret.Data[tlsKeyKey]
	if !ok || len(keyPEM) == 0 {
		return nil, nil, fmt.Errorf("secret %q missing %q key", secretName, tlsKeyKey)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse TLS certificate from Secret %q: %w", secretName, err)
	}

	// ca.crt is optional (cert-manager includes it when the issuer provides a CA)
	caCert := secret.Data[tlsCACertKey]

	return &cert, caCert, nil
}

// Close stops the informer and releases resources.
func (s *K8sSecretStore) Close() {
	close(s.stopCh)
}
