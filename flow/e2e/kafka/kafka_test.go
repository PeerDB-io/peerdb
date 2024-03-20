package e2e_kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/e2e"
	"github.com/PeerDB-io/peer-flow/e2eshared"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type KafkaSuite struct {
	t      *testing.T
	conn   *connpostgres.PostgresConnector
	suffix string
}

func (s KafkaSuite) T() *testing.T {
	return s.t
}

func (s KafkaSuite) Connector() *connpostgres.PostgresConnector {
	return s.conn
}

func (s KafkaSuite) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s KafkaSuite) Suffix() string {
	return s.suffix
}

func (s KafkaSuite) Peer() *protos.Peer {
	return &protos.Peer{
		Name: e2e.AddSuffix(s, "kasimple"),
		Type: protos.DBType_KAFKA,
		Config: &protos.Peer_KafkaConfig{
			KafkaConfig: &protos.KafkaConfig{
				Servers:    []string{"localhost:9092"},
				DisableTls: true,
			},
		},
	}
}

func (s KafkaSuite) DestinationTable(table string) string {
	return table
}

func (s KafkaSuite) Teardown() {
	e2e.TearDownPostgres(s)
}

func SetupSuite(t *testing.T) KafkaSuite {
	t.Helper()

	suffix := "ka_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return KafkaSuite{
		t:      t,
		conn:   conn,
		suffix: suffix,
	}
}

func Test_Kafka(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s KafkaSuite) TestSimple() {
	srcTableName := e2e.AttachSchema(s, "kasimple")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `insert into public.scripts (name, lang, source) values
	('e2e_kasimple', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      e2e.AddSuffix(s, "kasimple"),
		TableNameMapping: map[string]string{srcTableName: "katest"},
		Destination:      s.Peer(),
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs()
	flowConnConfig.Script = "e2e_kasimple"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, connectionGen)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics("katest"),
		)
		if err != nil {
			return false
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			require.Equal(s.t, "katest", ft.Topic)
			ft.EachRecord(func(r *kgo.Record) {
				require.Equal(s.t, "testval", string(r.Value))
			})
		})
		return true
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
