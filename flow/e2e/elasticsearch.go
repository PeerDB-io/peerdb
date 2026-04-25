package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

//nolint:govet // test fixture keeps related search settings grouped for readability.
type elasticsearchSuite struct {
	searchAddrs []string
	searchURL   string
	searchUser  string
	searchPass  string
	suffix      string
	backend     string
	t           *testing.T
	conn        *connpostgres.PostgresConnector
	searchAuth  protos.ElasticsearchAuthType
}

func (s elasticsearchSuite) T() *testing.T {
	return s.t
}

func (s elasticsearchSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s elasticsearchSuite) Source() SuiteSource {
	return &PostgresSource{PostgresConnector: s.conn}
}

func (s elasticsearchSuite) Suffix() string {
	return s.suffix
}

func SetupElasticSuite(t *testing.T) elasticsearchSuite {
	t.Helper()
	return setupSearchSuite(t, "elasticsearch", "es", "ELASTICSEARCH_TEST_ADDRESS")
}

func SetupOpenSearchSuite(t *testing.T) elasticsearchSuite {
	t.Helper()
	return setupSearchSuite(t, "opensearch", "os", "OPENSEARCH_TEST_ADDRESS")
}

func setupSearchSuite(t *testing.T, backend string, prefix string, addressEnv string) elasticsearchSuite {
	t.Helper()

	suffix := prefix + "_" + strings.ToLower(shared.RandomString(8))
	conn, err := SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	addressCSV := internal.GetEnvString(addressEnv, "")
	require.NotEmpty(t, addressCSV, "expected %s to be set", addressEnv)
	searchAddrs := strings.Split(addressCSV, ",")

	usernameEnv := strings.ToUpper(backend) + "_TEST_USERNAME"
	passwordEnv := strings.ToUpper(backend) + "_TEST_PASSWORD"
	searchUser := internal.GetEnvString(usernameEnv, "")
	searchPass := internal.GetEnvString(passwordEnv, "")
	searchAuth := protos.ElasticsearchAuthType_NONE
	if searchUser != "" || searchPass != "" {
		searchAuth = protos.ElasticsearchAuthType_BASIC
	}

	return elasticsearchSuite{
		t:           t,
		conn:        conn.PostgresConnector,
		suffix:      suffix,
		backend:     backend,
		searchURL:   strings.TrimRight(searchAddrs[0], "/"),
		searchAuth:  searchAuth,
		searchUser:  searchUser,
		searchPass:  searchPass,
		searchAddrs: searchAddrs,
	}
}

func (s elasticsearchSuite) Teardown(ctx context.Context) {
	TearDownPostgres(ctx, s)
}

func (s elasticsearchSuite) Peer() *protos.Peer {
	config := &protos.ElasticsearchConfig{
		Addresses: s.searchAddrs,
		AuthType:  s.searchAuth,
	}
	if s.searchAuth == protos.ElasticsearchAuthType_BASIC {
		config.Username = &s.searchUser
		config.Password = &s.searchPass
	}

	ret := &protos.Peer{
		Name: AddSuffix(s, s.backend),
		Type: protos.DBType_ELASTICSEARCH,
		Config: &protos.Peer_ElasticsearchConfig{
			ElasticsearchConfig: config,
		},
	}
	CreatePeer(s.t, ret)
	return ret
}

func (s elasticsearchSuite) countDocumentsInIndex(index string) int64 {
	status, payload := s.performSearchRequest(s.t.Context(), http.MethodPost, "/"+index+"/_count", nil)
	if status == http.StatusNotFound ||
		strings.Contains(string(payload), "index_not_found_exception") ||
		strings.Contains(string(payload), "search_phase_execution_exception") {
		return -1
	}

	require.Lessf(s.t, status, http.StatusMultipleChoices,
		"failed to get count of documents in index %s: %s", index, string(payload))

	var response struct {
		Count int64 `json:"count"`
	}
	require.NoError(s.t, json.Unmarshal(payload, &response), "failed to decode count response")
	return response.Count
}

func (s elasticsearchSuite) performSearchRequest(
	ctx context.Context,
	method string,
	path string,
	body io.Reader,
) (int, []byte) {
	req, err := http.NewRequestWithContext(ctx, method, s.searchURL+path, body)
	require.NoError(s.t, err, "failed to create %s request for %s", method, path)
	if s.searchAuth == protos.ElasticsearchAuthType_BASIC {
		req.SetBasicAuth(s.searchUser, s.searchPass)
	}

	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 4},
	}
	resp, err := client.Do(req)
	require.NoError(s.t, err, "failed to perform %s request for %s", method, path)
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	require.NoError(s.t, err, "failed to read %s response for %s", method, path)
	return resp.StatusCode, payload
}
