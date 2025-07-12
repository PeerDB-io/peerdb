package e2e_opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type opensearchSuite struct {
	t                *testing.T
	conn             *connpostgres.PostgresConnector
	opensearchClient *opensearch.Client
	suffix           string
	addresses        []string
	username         string
	password         string
}

func (s opensearchSuite) Source() e2e.SuiteSource {
	return &e2e.PostgresSource{PostgresConnector: s.conn}
}

func (s opensearchSuite) T() *testing.T {
	return s.t
}

func (s opensearchSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s opensearchSuite) Suffix() string {
	return s.suffix
}

func SetupSuite(t *testing.T) opensearchSuite {
	t.Helper()
	suffix := "os_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")
	addressCSV := internal.GetEnvString("OPENSEARCH_TEST_ADDRESS", "")
	addresses := strings.Split(addressCSV, ",")
	username := internal.GetEnvString("OPENSEARCH_TEST_USERNAME", "")
	password := internal.GetEnvString("OPENSEARCH_TEST_PASSWORD", "")

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
		},
	})
	require.NoError(t, err, "failed to create opensearch client")

	return opensearchSuite{
		t:                t,
		conn:             conn.PostgresConnector,
		opensearchClient: client,
		suffix:           suffix,
		addresses:        addresses,
		username:         username,
		password:         password,
	}
}

func (s opensearchSuite) Peer() *protos.Peer {
	return &protos.Peer{
		Name: e2e.AddSuffix(s, "opensearch"),
		Type: protos.DBType_OPENSEARCH,
		Config: &protos.Peer_OpensearchConfig{
			OpensearchConfig: &protos.OpensearchConfig{
				Addresses: s.addresses,
				Username:  &s.username,
				Password:  &s.password,
			},
		},
	}
}

func (s opensearchSuite) Teardown(ctx context.Context) {
	e2e.TearDownPostgres(ctx, s)
}

func (s opensearchSuite) TearDownIndices() {
	// delete all indices
	// List all indices via GET /_cat/indices?format=json
	req, err := http.NewRequestWithContext(context.Background(), "GET", "/_cat/indices?format=json", nil)
	require.NoError(s.t, err, "failed to create cat indices request")

	res, err := s.opensearchClient.Transport.Perform(req)
	require.NoError(s.t, err, "failed to get indices")
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	require.NoError(s.t, err, "failed to read indices response")

	var indices []map[string]interface{}
	err = json.Unmarshal(body, &indices)
	require.NoError(s.t, err, "failed to unmarshal indices response")

	for _, index := range indices {
		indexName := index["index"].(string)
		delReq, err := http.NewRequestWithContext(context.Background(), "DELETE", "/"+indexName, nil)
		require.NoError(s.t, err, fmt.Sprintf("failed to create delete request for index %s", indexName))
		delRes, err := s.opensearchClient.Transport.Perform(delReq)
		require.NoError(s.t, err, fmt.Sprintf("failed to delete index %s", indexName))
		delRes.Body.Close()
	}
}

func (s opensearchSuite) countDocumentsInIndex(indexName string) int64 {
	// Count documents via POST /{index}/_count
	req, err := http.NewRequestWithContext(context.Background(), "POST", "/"+indexName+"/_count", nil)
	require.NoError(s.t, err, "failed to create count request")

	res, err := s.opensearchClient.Transport.Perform(req)
	require.NoError(s.t, err, "failed to count documents in index")
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	require.NoError(s.t, err, "failed to read count response")

	var countResponse map[string]interface{}
	err = json.Unmarshal(body, &countResponse)
	require.NoError(s.t, err, "failed to unmarshal count response")

	return int64(countResponse["count"].(float64))
}
