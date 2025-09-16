package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

func (s KafkaSuite) Source() SuiteSource {
	return &PostgresSource{PostgresConnector: s.conn}
}

func (s KafkaSuite) Conn() *pgx.Conn {
	return s.Connector().Conn()
}

func (s KafkaSuite) Suffix() string {
	return s.suffix
}

func (s KafkaSuite) Peer() *protos.Peer {
	ret := &protos.Peer{
		Name: AddSuffix(s, "kafka"),
		Type: protos.DBType_KAFKA,
		Config: &protos.Peer_KafkaConfig{
			KafkaConfig: &protos.KafkaConfig{
				Servers:    []string{"localhost:9092"},
				DisableTls: true,
			},
		},
	}
	CreatePeer(s.t, ret)
	return ret
}

func (s KafkaSuite) DestinationTable(table string) string {
	return table
}

func (s KafkaSuite) Teardown(ctx context.Context) {
	TearDownPostgres(ctx, s)
}

func SetupKafkaSuite(t *testing.T) KafkaSuite {
	t.Helper()

	suffix := "ka_" + strings.ToLower(shared.RandomString(8))
	conn, err := SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return KafkaSuite{
		t:      t,
		conn:   conn.PostgresConnector,
		suffix: suffix,
	}
}

func Test_Kafka(t *testing.T) {
	e2eshared.RunSuite(t, SetupKafkaSuite)
}

func (s KafkaSuite) TestSimple() {
	srcTableName := AttachSchema(s, "kasimple")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('e2e_kasimple', 'lua', 'function onRecord(r) return r.row and r.row.val end') on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := AddSuffix(s, "kasimple")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_kasimple"

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'testval')`, srcTableName))
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(s.t.Context(), time.Minute)
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
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestMessage() {
	srcTableName := AttachSchema(s, "kamessage")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				val text
			);
		`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `INSERT INTO public.scripts (name, lang, source) values ('e2e_kamessage', 'lua',
	'function onRecord(r) if r.kind == "message" then return { topic = r.prefix, value = r.content } end end'
	) ON CONFLICT DO NOTHING`)
	require.NoError(s.t, err)

	flowName := AddSuffix(s, "kamessage")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_kamessage"

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	// insert record to bypass empty cdc store optimization which handles messages at source when message precedes records
	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf("insert into %s(val) values ('tick')", srcTableName))
	require.NoError(s.t, err)
	_, err = s.Conn().Exec(s.t.Context(), "SELECT pg_logical_emit_message(false, 'topic', '\\x686561727462656174'::bytea)")
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize message", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics("topic"),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(s.t.Context(), time.Minute)
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
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestDefault() {
	srcTableName := AttachSchema(s, "kadefault")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	flowName := AddSuffix(s, "kadefault")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'testval')`, srcTableName))
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(s.t.Context(), time.Minute)
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
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestInitialLoad() {
	srcTableName := AttachSchema(s, "kainitial")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	flowName := AddSuffix(s, "kainitial")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'testval')`, srcTableName))
	require.NoError(s.t, err)

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(s.t.Context(), time.Minute)
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
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}

func (s KafkaSuite) TestOriginMetadata() {
	srcTableName := AttachSchema(s, "kaorigin")

	_, err := s.Conn().Exec(s.t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			val text
		);
	`, srcTableName))
	require.NoError(s.t, err)

	_, err = s.Conn().Exec(s.t.Context(), `insert into public.scripts (name, lang, source) values
	('e2e_kaorigin', 'lua',
		'function onRecord(r) return peerdb.type(r.transaction_id) .. tostring(r.row._peerdb_origin_transaction_id == r.transaction_id) end')
		on conflict do nothing`)
	require.NoError(s.t, err)

	flowName := AddSuffix(s, "kaorigin")
	connectionGen := FlowConnectionGenerationConfig{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{srcTableName: flowName},
		Destination:      s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.Script = "e2e_kaorigin"
	flowConnConfig.Env = map[string]string{"PEERDB_ORIGIN_METADATA_AS_DESTINATION_COLUMN": "true"}

	tc := NewTemporalClient(s.t)
	env := ExecutePeerflow(s.t, tc, flowConnConfig)
	SetupCDCFlowStatusQuery(s.t, env, flowConnConfig)

	_, err = s.Conn().Exec(s.t.Context(), fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'testval')`, srcTableName))
	require.NoError(s.t, err)

	EnvWaitFor(s.t, env, 3*time.Minute, "normalize insert", func() bool {
		kafka, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ConsumeTopics(flowName),
		)
		if err != nil {
			return false
		}
		defer kafka.Close()

		ctx, cancel := context.WithTimeout(s.t.Context(), time.Minute)
		defer cancel()
		fetches := kafka.PollFetches(ctx)
		fetches.EachTopic(func(ft kgo.FetchTopic) {
			ft.EachRecord(func(r *kgo.Record) {
				require.Equal(s.t, "uint64true", string(r.Value))
			})
		})
		return true
	})
	env.Cancel(s.t.Context())
	RequireEnvCanceled(s.t, env)
}
