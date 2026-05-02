package connbigquery

import (
	"context"
	"net/http"
	"sync"

	"cloud.google.com/go/auth"
)

// GCSHeaderProvider provides HTTP headers for authenticating GCS object requests.
// It caches tokens and refreshes them when they expire.
//
//nolint:govet // only expecting a handful of these at a time
type GCSHeaderProvider struct {
	connector  *BigQueryConnector
	bucketName string
	prefix     string

	mu    sync.Mutex
	token *auth.Token
}

func NewGCSHeaderProvider(c *BigQueryConnector, bucket, prefix string) *GCSHeaderProvider {
	return &GCSHeaderProvider{
		connector:  c,
		bucketName: bucket,
		prefix:     prefix,
	}
}

func (p *GCSHeaderProvider) GetHeaders(ctx context.Context) (http.Header, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token == nil || !p.token.IsValid() {
		token, err := p.connector.storageDownScopedToken(ctx, p.bucketName, p.prefix)
		if err != nil {
			return nil, err
		}
		p.token = token
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+p.token.Value)
	return headers, nil
}

func (p *GCSHeaderProvider) InvalidateToken() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.token = nil
}
