package e2e_elasticsearch

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

type elasticsearchSuite struct {
	t        *testing.T
	conn     *connpostgres.PostgresConnector
	esClient *elasticsearch.TypedClient
	peer     *protos.Peer
	suffix   string
}

func (s elasticsearchSuite) T() *testing.T {
	return s.t
}

func (s elasticsearchSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s elasticsearchSuite) Suffix() string {
	return s.suffix
}

func SetupSuite(t *testing.T) elasticsearchSuite {
	t.Helper()

	suffix := "es_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")
	esAddresses := strings.Split(peerdbenv.GetEnvString("ELASTICSEARCH_TEST_ADDRESS", ""), ",")

	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: esAddresses,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
		},
	})
	require.NoError(t, err, "failed to setup elasticsearch")

	suite := elasticsearchSuite{
		t:        t,
		conn:     conn,
		esClient: esClient,
		suffix:   suffix,
	}
	suite.peer = &protos.Peer{
		Name: e2e.AddSuffix(suite, "elasticsearch"),
		Type: protos.DBType_ELASTICSEARCH,
		Config: &protos.Peer_ElasticsearchConfig{
			ElasticsearchConfig: &protos.ElasticsearchConfig{
				Addresses: esAddresses,
				AuthType:  protos.ElasticsearchAuthType_NONE,
			},
		},
	}
	e2e.CreatePeer(t, suite.peer)

	return suite
}

func (s elasticsearchSuite) Teardown() {
	e2e.TearDownPostgres(s)
}

func (s elasticsearchSuite) Peer() *protos.Peer {
	return s.peer
}

func (s elasticsearchSuite) countDocumentsInIndex(index string) int64 {
	res, err := s.esClient.Count().Index(index).Do(context.Background())
	// index may not exist yet, don't error out for that
	// search can occasionally fail, retry for that
	if err != nil && (strings.Contains(err.Error(), "index_not_found_exception") ||
		strings.Contains(err.Error(), "search_phase_execution_exception")) {
		return -1
	}
	require.NoError(s.t, err, "failed to get count of documents in index")
	return res.Count
}
