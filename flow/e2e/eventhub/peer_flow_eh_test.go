package e2e_eventhubs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type EventhubsSuite struct {
	t          *testing.T
	conn       *connpostgres.PostgresConnector
	suffix     string
	timeSuffix string
}

func (s EventhubsSuite) T() *testing.T {
	return s.t
}

func (s EventhubsSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s EventhubsSuite) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s EventhubsSuite) Suffix() string {
	return s.suffix
}

func EventhubsCreds() (*protos.EventHubConfig, error) {
	jsonPath := os.Getenv("TEST_EH_CREDS")
	if jsonPath == "" {
		return nil, errors.New("TEST_EH_CREDS env var not set")
	}

	content, err := e2eshared.ReadFileToBytes(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var config *protos.EventHubConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	return config, nil
}

func (s EventhubsSuite) Peer(config *protos.EventHubConfig) *protos.Peer {
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "eventhubs"),
		Type: protos.DBType_EVENTHUBS,
		Config: &protos.Peer_EventhubGroupConfig{
			EventhubGroupConfig: &protos.EventHubGroupConfig{
				// map of string to EventhubConfig
				Eventhubs: map[string]*protos.EventHubConfig{
					config.Namespace: {
						Namespace:              config.Namespace,
						ResourceGroup:          config.ResourceGroup,
						SubscriptionId:         config.SubscriptionId,
						Location:               config.Location,
						PartitionCount:         5,
						MessageRetentionInDays: 1,
					},
				},
			},
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
}

func (s EventhubsSuite) GetEventhubName() string {
	eventhubName := fmt.Sprintf("eh_%s_%s",
		strings.ToLower(s.suffix), s.timeSuffix)
	return eventhubName
}

func (s EventhubsSuite) Teardown() {
	e2e.TearDownPostgres(s)
	creds, err := EventhubsCreds()
	require.NoError(s.t, err)
	err = DeleteEventhub(context.Background(), s.GetEventhubName(), creds)
	require.NoError(s.t, err)
}

func SetupSuite(t *testing.T) EventhubsSuite {
	t.Helper()

	suffix := strings.ToLower(shared.RandomString(8))
	tsSuffix := time.Now().Format("20060102150405")
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return EventhubsSuite{
		t:          t,
		conn:       conn,
		suffix:     suffix,
		timeSuffix: tsSuffix,
	}
}

func Test_Eventhubs(t *testing.T) {
	t.Skip()
	e2eshared.RunSuite(t, SetupSuite)
}

func (s EventhubsSuite) Test_EH_Simple() {
	srcTableName := e2e.AttachSchema(s, "eh_simple")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	ehCreds, err := EventhubsCreds()
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('e2e_eh_simple_script', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "e2e_eh_simple")
	scopedEventhubName := fmt.Sprintf("%s.%s.id",
		ehCreds.Namespace, s.GetEventhubName())
	destinationPeer := s.Peer(ehCreds)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: scopedEventhubName},
		Destination:      destinationPeer.Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.Script = "e2e_eh_simple_script"
	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	e2e.EnvNoError(s.t, env, err)

	e2e.EnvWaitFor(s.t, env, 2*time.Minute, "eventhubs message check", func() bool {
		messages, err := ConsumeAllMessages(
			context.Background(),
			ehCreds.Namespace,
			s.GetEventhubName(),
			1,
			destinationPeer.GetEventhubGroupConfig(),
		)
		if err != nil {
			return false
		}

		msgCountIsOne := len(messages) == 1
		msgValueCheck := messages[0] == "testval"
		return msgCountIsOne && msgValueCheck
	})

	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
