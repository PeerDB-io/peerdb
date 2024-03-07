package connkafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
	"github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
)

type KafkaConnector struct {
	client     *kgo.Client
	pgMetadata *metadataStore.PostgresMetadataStore
	logger     log.Logger
}

func NewKafkaConnector(
	ctx context.Context,
	config *protos.KafkaConfig,
) (*KafkaConnector, error) {
	optionalOpts := append(
		make([]kgo.Opt, 0, 6),
		kgo.SeedBrokers(config.Servers...),
		kgo.AllowAutoTopicCreation(),
		kgo.WithLogger(kslog.New(slog.Default())), // TODO use logger.LoggerFromCtx
		kgo.SoftwareNameAndVersion("peerdb", peerdbenv.PeerDBVersionShaShort()),
	)
	if !config.DisableTls {
		optionalOpts = append(optionalOpts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
	}
	if config.Username != "" {
		auth := scram.Auth{User: config.Username, Pass: config.Password}
		switch config.Sasl {
		case "SCRAM-SHA-256":
			optionalOpts = append(optionalOpts, kgo.SASL(auth.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			optionalOpts = append(optionalOpts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", config.Sasl)
		}
	}
	client, err := kgo.NewClient(optionalOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &KafkaConnector{
		client: client,
		logger: logger.LoggerFromCtx(ctx),
	}, nil
}

func (c *KafkaConnector) Close() error {
	if c != nil {
		c.client.Close()
	}
	return nil
}

func (c *KafkaConnector) ConnectionActive(ctx context.Context) error {
	return c.client.Ping(ctx)
}

func (c *KafkaConnector) CreateRawTable(ctx context.Context, req *protos.CreateRawTableInput) (*protos.CreateRawTableOutput, error) {
	return &protos.CreateRawTableOutput{TableIdentifier: "n/a"}, nil
}

func (c *KafkaConnector) GetLastSyncBatchID(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.GetLastBatchID(ctx, jobName)
}

func (c *KafkaConnector) GetLastOffset(ctx context.Context, jobName string) (int64, error) {
	return c.pgMetadata.FetchLastOffset(ctx, jobName)
}

func (c *KafkaConnector) SetLastOffset(ctx context.Context, jobName string, offset int64) error {
	return c.pgMetadata.UpdateLastOffset(ctx, jobName, offset)
}

func (c *KafkaConnector) NeedsSetupMetadataTables(_ context.Context) bool {
	return false
}

func (c *KafkaConnector) SetupMetadataTables(_ context.Context) error {
	return nil
}

func (c *KafkaConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	c.logger.Info("ReplayTableSchemaDeltas for event hub is a no-op")
	return nil
}

func (c *KafkaConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	return c.pgMetadata.DropMetadata(ctx, jobName)
}

func (c *KafkaConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	err := c.client.BeginTransaction()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wgCtx, wgErr := context.WithCancelCause(ctx)
	produceCb := func(r *kgo.Record, err error) {
		if err != nil {
			wgErr(err)
		}
		wg.Done()
	}

	numRecords := int64(0)
	tableNameRowsMapping := make(map[string]uint32)

	var fn *lua.LFunction
	var ls *lua.LState
	if req.Script != "" {
		ls = lua.NewState(lua.Options{SkipOpenLibs: true})
		defer ls.Close()
		ls.SetContext(wgCtx)
		for _, pair := range []struct {
			n string
			f lua.LGFunction
		}{
			{lua.LoadLibName, lua.OpenPackage}, // Must be first
			{lua.BaseLibName, lua.OpenBase},
			{lua.TabLibName, lua.OpenTable},
			{lua.StringLibName, lua.OpenString},
			{lua.MathLibName, lua.OpenMath},
		} {
			ls.Push(ls.NewFunction(pair.f))
			ls.Push(lua.LString(pair.n))
			err := ls.PCall(1, 0, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize Lua runtime: %w", err)
			}
		}
		ls.PreloadModule("flatbuffers", pua.FlatBuffers_Loader)
		pua.RegisterTypes(ls)
		err := ls.DoString(req.Script)
		if err != nil {
			return nil, fmt.Errorf("error while executing script: %w", err)
		}

		var ok bool
		fn, ok = ls.GetGlobal("onRow").(*lua.LFunction)
		if !ok {
			return nil, errors.New("script should define `onRow` function")
		}
	} else {
		return nil, errors.New("kafka mirror must have script")
	}

	for record := range req.Records.GetRecords() {
		if err := wgCtx.Err(); err != nil {
			return nil, err
		}
		topic := record.GetDestinationTableName()
		ls.Push(fn)
		ls.Push(pua.LuaRecord.New(ls, record))
		err := ls.PCall(1, 1, nil)
		if err != nil {
			return nil, fmt.Errorf("script failed: %w", err)
		}
		value := ls.Get(-1)
		if value != lua.LNil {
			lstr, ok := value.(lua.LString)
			if !ok {
				return nil, fmt.Errorf("script returned non-nil non-string: %v", value)
			}
			wg.Add(1)
			c.client.Produce(wgCtx, &kgo.Record{Topic: topic, Value: bytes.Clone([]byte(lstr))}, produceCb)

			numRecords += 1
			tableNameRowsMapping[topic] += 1
		}
	}

	// TODO handle
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		waitChan <- struct{}{}
	}()
	select {
	case <-wgCtx.Done():
		return nil, wgCtx.Err()
	case <-waitChan:
	}

	if err := c.client.Flush(ctx); err != nil {
		return nil, fmt.Errorf("could not flush transaction: %w", err)
	}

	if err := c.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	lastCheckpoint := req.Records.GetLastCheckpoint()
	err = c.pgMetadata.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint)
	if err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       numRecords,
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}
