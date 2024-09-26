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
	ret := &protos.Peer{
		Name: e2e.AddSuffix(s, "kafka"),
		Type: protos.DBType_KAFKA,
		Config: &protos.Peer_KafkaConfig{
			KafkaConfig: &protos.KafkaConfig{
				Servers:    []string{"localhost:9092"},
				DisableTls: true,
			},
		},
	}
	e2e.CreatePeer(s.t, ret)
	return ret
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

	flowName := e2e.AddSuffix(s, "kasimple")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.Script = "e2e_kasimple"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			require.Equal(s.t, flowName, ft.Topic)
			ft.EachRecord(func(r *kgo.Record) {
				require.Equal(s.t, "testval", string(r.Value))
			})
		})
		return true
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestMessage() {
	srcTableName := e2e.AttachSchema(s, "kamessage")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				val text
			);
		`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(context.Background(), `INSERT INTO public.scripts (name, lang, source) values ('e2e_kamessage', 'lua',
	'function onRecord(r) if r.kind == "message" then return { topic = r.prefix, value = r.content } end end'
	) ON CONFLICT DO NOTHING`)
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "kamessage")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.Script = "e2e_kamessage"

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// insert record to bypass empty cdc store optimization which handles messages at source when message precedes records
	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf("insert into %s(val) values ('tick')", srcTableName))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(context.Background(), "SELECT pg_logical_emit_message(false, 'topic', '\\x686561727462656174'::bytea)")
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize message", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics("topic"),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			require.Equal(s.t, "topic", ft.Topic)
			ft.EachRecord(func(r *kgo.Record) {
				require.Equal(s.t, "heartbeat", string(r.Value))
			})
		})
		return true
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestDefault() {
	srcTableName := e2e.AttachSchema(s, "kadefault")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "kadefault")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			require.Equal(s.t, flowName, ft.Topic)
			ft.EachRecord(func(r *kgo.Record) {
				require.Contains(s.t, string(r.Value), "\"testval\"")
				require.Equal(s.t, byte('{'), r.Value[0])
				require.Equal(s.t, byte('}'), r.Value[len(r.Value)-1])
			})
		})
		return true
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestInitialLoad() {
	srcTableName := e2e.AttachSchema(s, "kainitial")

	_, err := s.Conn().Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	flowName := e2e.AddSuffix(s, "kainitial")
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s.t)
	flowConnConfig.DoInitialSnapshot = true

	_, err = s.Conn().Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, val) VALUES (1, 'testval')
	`, srcTableName))
	require.NoError(s.t, err)

	tc := e2e.NewTemporalClient(s.t)
	env := e2e.ExecutePeerflow(tc, peerflow.CDCFlowWorkflow, flowConnConfig, nil)
	e2e.SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	e2e.EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			require.Equal(s.t, flowName, ft.Topic)
			ft.EachRecord(func(r *kgo.Record) {
				require.Contains(s.t, string(r.Value), "\"testval\"")
				require.Equal(s.t, byte('{'), r.Value[0])
				require.Equal(s.t, byte('}'), r.Value[len(r.Value)-1])
			})
		})
		return true
	})
	env.Cancel()
	e2e.RequireEnvCanceled(s.t, env)
}
