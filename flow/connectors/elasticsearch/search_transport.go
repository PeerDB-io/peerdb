package connelasticsearch

import (
	"crypto/tls"
	"net/http"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func searchHTTPTransport() http.RoundTripper {
	return &http.Transport{
		MaxIdleConnsPerHost: 4,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS13},
	}
}

//nolint:govet // auth transport state is tiny and readability matters more than packing.
type searchAuthTransport struct {
	username string
	password string
	apiKey   string
	base     http.RoundTripper
	authType protos.ElasticsearchAuthType
}

func newSearchAuthTransport(config *protos.ElasticsearchConfig, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &searchAuthTransport{
		base:     base,
		authType: config.AuthType,
		username: config.GetUsername(),
		password: config.GetPassword(),
		apiKey:   config.GetApiKey(),
	}
}

func (t *searchAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()

	switch t.authType {
	case protos.ElasticsearchAuthType_BASIC:
		cloned.SetBasicAuth(t.username, t.password)
	case protos.ElasticsearchAuthType_APIKEY:
		cloned.Header.Set("Authorization", "ApiKey "+t.apiKey)
	}

	return t.base.RoundTrip(cloned)
}
