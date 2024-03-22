package connkafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
	lua "github.com/yuin/gopher-lua"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
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
		make([]kgo.Opt, 0, 7),
		kgo.SeedBrokers(config.Servers...),
		kgo.AllowAutoTopicCreation(),
		kgo.WithLogger(kslog.New(slog.Default())), // TODO use logger.LoggerFromCtx
		kgo.SoftwareNameAndVersion("peerdb", peerdbenv.PeerDBVersionShaShort()),
	)
	if !config.DisableTls {
		optionalOpts = append(optionalOpts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
	}
	switch config.Partitioner {
	case "LeastBackup":
		optionalOpts = append(optionalOpts, kgo.RecordPartitioner(kgo.LeastBackupPartitioner()))
	case "Manual":
		optionalOpts = append(optionalOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	case "RoundRobin":
		optionalOpts = append(optionalOpts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	case "StickyKey":
		optionalOpts = append(optionalOpts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	case "Sticky":
		optionalOpts = append(optionalOpts, kgo.RecordPartitioner(kgo.StickyPartitioner()))
	}
	if config.Username != "" {
		switch config.Sasl {
		case "PLAIN":
			auth := plain.Auth{User: config.Username, Pass: config.Password}
			optionalOpts = append(optionalOpts, kgo.SASL(auth.AsMechanism()))
		case "SCRAM-SHA-256":
			auth := scram.Auth{User: config.Username, Pass: config.Password}
			optionalOpts = append(optionalOpts, kgo.SASL(auth.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			auth := scram.Auth{User: config.Username, Pass: config.Password}
			optionalOpts = append(optionalOpts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", config.Sasl)
		}
	}
	client, err := kgo.NewClient(optionalOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		return nil, err
	}

	return &KafkaConnector{
		client:     client,
		pgMetadata: pgMetadata,
		logger:     logger.LoggerFromCtx(ctx),
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
	return nil
}

func (c *KafkaConnector) SyncFlowCleanup(ctx context.Context, jobName string) error {
	return c.pgMetadata.DropMetadata(ctx, jobName)
}

func lvalueToKafkaRecord(ls *lua.LState, value lua.LValue) (*kgo.Record, error) {
	var kr *kgo.Record
	switch v := value.(type) {
	case lua.LString:
		kr = kgo.StringRecord(string(v))
	case *lua.LTable:
		key, err := utils.LVAsReadOnlyBytes(ls, ls.GetField(v, "key"))
		if err != nil {
			return nil, fmt.Errorf("invalid key, %w", err)
		}
		value, err := utils.LVAsReadOnlyBytes(ls, ls.GetField(v, "value"))
		if err != nil {
			return nil, fmt.Errorf("invalid value, %w", err)
		}
		topic, err := utils.LVAsStringOrNil(ls, ls.GetField(v, "topic"))
		if err != nil {
			return nil, fmt.Errorf("invalid topic, %w", err)
		}
		partition := int32(lua.LVAsNumber(ls.GetField(v, "partition")))
		kr = &kgo.Record{
			Key:       key,
			Value:     value,
			Topic:     topic,
			Partition: partition,
		}
		lheaders := ls.GetField(v, "headers")
		if headers, ok := lheaders.(*lua.LTable); ok {
			headers.ForEach(func(k, v lua.LValue) {
				kstr := k.String()
				vbytes, err := utils.LVAsReadOnlyBytes(ls, v)
				if err != nil {
					vbytes = shared.UnsafeFastStringToReadOnlyBytes(err.Error())
				}
				kr.Headers = append(kr.Headers, kgo.RecordHeader{
					Key:   kstr,
					Value: vbytes,
				})
			})
		} else if lua.LVAsBool(lheaders) {
			return nil, fmt.Errorf("invalid headers, must be nil or table: %s", lheaders)
		}
	case *lua.LNilType:
	default:
		return nil, fmt.Errorf("script returned invalid value: %s", value)
	}
	return kr, nil
}

func (c *KafkaConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest) (*model.SyncResponse, error) {
	var wg sync.WaitGroup
	wgCtx, wgErr := context.WithCancelCause(ctx)
	produceCb := func(r *kgo.Record, err error) {
		if err != nil {
			wgErr(err)
		}
		wg.Done()
	}

	numRecords := int64(0)
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)

	ls, err := utils.LoadScript(wgCtx, req.Script, func(ls *lua.LState) int {
		top := ls.GetTop()
		ss := make([]string, top)
		for i := range top {
			ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
		}
		_ = c.pgMetadata.LogFlowInfo(ctx, req.FlowJobName, strings.Join(ss, "\t"))
		return 0
	})
	if err != nil {
		return nil, err
	}
	defer ls.Close()

	lfn := ls.Env.RawGetString("onRecord")
	fn, ok := lfn.(*lua.LFunction)
	if !ok {
		return nil, fmt.Errorf("script should define `onRecord` as function, not %s", lfn)
	}

	for record := range req.Records.GetRecords() {
		if err := wgCtx.Err(); err != nil {
			return nil, err
		}
		ls.Push(fn)
		ls.Push(pua.LuaRecord.New(ls, record))
		err := ls.PCall(1, -1, nil)
		if err != nil {
			return nil, fmt.Errorf("script failed: %w", err)
		}
		args := ls.GetTop()
		for i := range args {
			kr, err := lvalueToKafkaRecord(ls, ls.Get(i-args))
			if err != nil {
				return nil, err
			}
			if kr != nil {
				if kr.Topic == "" {
					kr.Topic = record.GetDestinationTableName()
				}

				wg.Add(1)
				c.client.Produce(wgCtx, kr, produceCb)
				switch record.(type) {
				case *model.InsertRecord:
					tableNameRowsMapping[kr.Topic].InsertCount += 1
				case *model.UpdateRecord:
					tableNameRowsMapping[kr.Topic].UpdateCount += 1
				case *model.DeleteRecord:
					tableNameRowsMapping[kr.Topic].DeleteCount += 1
				}
			}
		}
		numRecords += 1
		ls.SetTop(0)
	}

	if err := c.client.Flush(ctx); err != nil {
		return nil, fmt.Errorf("could not flush transaction: %w", err)
	}
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()
	select {
	case <-wgCtx.Done():
		return nil, wgCtx.Err()
	case <-waitChan:
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
