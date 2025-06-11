package e2e_pubsub

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

	peer_bq "github.com/PeerDB-io/peerdb/flow/connectors/bigquery"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
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

func (s PubSubSuite) Source() e2e.SuiteSource {
	return &e2e.PostgresSource{PostgresConnector: s.conn}
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

func (s PubSubSuite) Peer(sa *utils.GcpServiceAccount) *protos.Peer {
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "pubsub"),
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
	e2e.CreatePeer(s.t, ret)
	return ret
}

func (s PubSubSuite) DestinationTable(table string) string {
	return table
}

func (s PubSubSuite) Teardown(ctx context.Context) {
	e2e.TearDownPostgres(ctx, s)
}

func SetupSuite(t *testing.T) PubSubSuite {
	t.Helper()

	suffix := "ps_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return PubSubSuite{
		t:      t,
		conn:   conn.PostgresConnector,
		suffix: suffix,
	}
}

func Test_PubSub(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PubSubSuite) TestCreateTopic() {
	srcTableName := e2e.AttachSchema(s, "pscreate")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	sa, err := ServiceAccount()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('e2e_pscreate', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "e2epscreate")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer(sa).Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_pscreate"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "create topic", func() bool {
		psclient, err := sa.CreatePubSubClient(s.t.Context())
		defer func() {
			_ = psclient.Close()
		}()
		require.NoError(s.t, err)
		topic := psclient.Topic(flowName)
		exists, err := topic.Exists(s.t.Context())
		require.NoError(s.t, err)
		return exists
	})

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PubSubSuite) TestSimple() {
	srcTableName := e2e.AttachSchema(s, "pssimple")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	sa, err := ServiceAccount()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('e2e_pssimple', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "e2epssimple")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer(sa).Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_pssimple"

	psclient, err := sa.CreatePubSubClient(s.t.Context())
	require.NoError(s.t, err)
	defer psclient.Close()
	topic, err := psclient.CreateTopic(s.t.Context(), flowName)
	require.NoError(s.t, err)
	sub, err := psclient.CreateSubscription(s.t.Context(), flowName, pubsub.SubscriptionConfig{
		Topic:             topic,
		RetentionDuration: 10 * time.Minute,
		ExpirationPolicy:  24 * time.Hour,
	})
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	ctx, cancel := context.WithTimeout(s.t.Context(), 3*time.Minute)
	defer cancel()

	msgs := make(chan *pubsub.Message)
	go func() {
		_ = sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
			msgs <- m
		})
	}()
	select {
	case msg := <-msgs:
		require.NotNil(s.t, msg)
		require.Equal(s.t, "testval", string(msg.Data))
	case <-ctx.Done():
		s.t.Log("UNEXPECTED TIMEOUT PubSub subscription waiting on message")
		s.t.Fail()
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}

func (s PubSubSuite) TestInitialLoad() {
	srcTableName := e2e.AttachSchema(s, "psinitial")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	sa, err := ServiceAccount()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('e2e_psinitial', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "e2epsinitial")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer(sa).Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_psinitial"
	flowConnConfig.DoInitialSnapshot = true

	psclient, err := sa.CreatePubSubClient(s.t.Context())
	require.NoError(s.t, err)
	defer psclient.Close()
	topic, err := psclient.CreateTopic(s.t.Context(), flowName)
	require.NoError(s.t, err)
	sub, err := psclient.CreateSubscription(s.t.Context(), flowName, pubsub.SubscriptionConfig{
		Topic:             topic,
		RetentionDuration: 10 * time.Minute,
		ExpirationPolicy:  24 * time.Hour,
	})
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(s.t.Context(), tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	ctx, cancel := context.WithTimeout(s.t.Context(), 3*time.Minute)
	defer cancel()

	msgs := make(chan *pubsub.Message)
	go func() {
		_ = sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
			msgs <- m
		})
	}()
	select {
	case msg := <-msgs:
		require.NotNil(s.t, msg)
		require.Equal(s.t, "testval", string(msg.Data))
	case <-ctx.Done():
		s.t.Log("UNEXPECTED TIMEOUT PubSub subscription waiting on message")
		s.t.Fail()
	}

	env.Cancel(s.t.Context())
	e2e.RequireEnvCanceled(s.t, env)
}
