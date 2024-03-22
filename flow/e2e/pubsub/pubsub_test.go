package e2e_kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	peer_bq "github.com/PeerDB-io/peer-flow/connectors/bigquery"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type PubSubSuite struct {
	t      *testing.T
	conn   *connpostgres.PostgresConnector
	suffix string
}

func (s PubSubSuite) T() *testing.T {
	return s.t
}

func (s PubSubSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s PubSubSuite) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s PubSubSuite) Suffix() string {
	return s.suffix
}

func ServiceAccount() (*utils.GcpServiceAccount, error) {
	jsonPath := os.Getenv("TEST_BQ_CREDS")
	if jsonPath == "" {
		return nil, errors.New("TEST_BQ_CREDS env var not set")
	}

	content, err := e2eshared.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config *protos.BigqueryConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	return peer_bq.NewBigQueryServiceAccount(config)
}

func (s PubSubSuite) Peer(name string, sa *utils.GcpServiceAccount) *protos.Peer {
	return &protos.Peer{
		Name: e2e.AddSuffix(s, name),
		Type: protos.DBType_PUBSUB,
		Config: &protos.Peer_PubsubConfig{
			PubsubConfig: &protos.PubSubConfig{
				ServiceAccount: &protos.GcpServiceAccount{
					AuthType:                sa.Type,
					ProjectId:               sa.ProjectID,
					PrivateKeyId:            sa.PrivateKeyID,
					PrivateKey:              sa.PrivateKey,
					ClientEmail:             sa.ClientEmail,
					ClientId:                sa.ClientID,
					AuthUri:                 sa.AuthURI,
					TokenUri:                sa.TokenURI,
					AuthProviderX509CertUrl: sa.AuthProviderX509CertURL,
					ClientX509CertUrl:       sa.ClientX509CertURL,
				},
			},
		},
	}
}

func (s PubSubSuite) DestinationTable(table string) string {
	return table
}

func (s PubSubSuite) Teardown() {
	e2e.TearDownPostgres(s)
}

func SetupSuite(t *testing.T) PubSubSuite {
	t.Helper()

	suffix := "ps_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return PubSubSuite{
		t:      t,
		conn:   conn,
		suffix: suffix,
	}
}

func Test_PubSub(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PubSubSuite) TestCreateTopic() {
	srcTableName := e2e.AttachSchema(s, "pscreate")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	sa, err := ServiceAccount()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('e2e_pscreate', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "pscreate")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: e2e.AddSuffix(s, flowName)},
		Destination:      s.Peer(flowName, sa),
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.Script = "e2e_pscreate"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	psclient, err := sa.CreatePubSubClient(context.Background())
	require.NoError(s.t, err)
	topic := psclient.Topic(flowName)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "create topic", func() bool {
		exists, err := topic.Exists(context.Background())
		require.NoError(s.t, err)
		return exists
	})
}

func (s PubSubSuite) TestSimple() {
	srcTableName := e2e.AttachSchema(s, "pssimple")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	sa, err := ServiceAccount()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('e2e_pssimple', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "pssimple")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: e2e.AddSuffix(s, flowName)},
		Destination:      s.Peer(flowName, sa),
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.Script = "e2e_pssimple"

	psclient, err := sa.CreatePubSubClient(context.Background())
	require.NoError(s.t, err)
	topic, err := psclient.CreateTopic(context.Background(), flowName)
	require.NoError(s.t, err)
	sub, err := psclient.CreateSubscription(context.Background(), flowName, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var msg *pubsub.Message
	_ = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		msg = m
		cancel()
	})
	require.NotNil(s.t, msg)
	require.Equal(s.t, "testval", string(msg.Data))

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
