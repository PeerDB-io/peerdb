package connkafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/twmb/franz-go/plugin/kslog"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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
	*metadataStore.PostgresMetadata
	client *kgo.Client
	logger log.Logger
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
		optionalOpts = append(optionalOpts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
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

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &KafkaConnector{
		PostgresMetadata: pgMetadata,
		client:           client,
		logger:           logger.LoggerFromCtx(ctx),
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

func (c *KafkaConnector) NeedsSetupMetadataTables(_ context.Context) bool {
	return false
}

func (c *KafkaConnector) SetupMetadataTables(_ context.Context) error {
	return nil
}

func (c *KafkaConnector) ReplayTableSchemaDeltas(_ context.Context, flowJobName string, schemaDeltas []*protos.TableSchemaDelta) error {
	return nil
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

func (c *KafkaConnector) SyncRecords(ctx context.Context, req *model.SyncRecordsRequest[model.RecordItems]) (*model.SyncResponse, error) {
	var wg sync.WaitGroup
	wgCtx, wgErr := context.WithCancelCause(ctx)
	produceCb := func(_ *kgo.Record, err error) {
		if err != nil {
			wgErr(err)
		}
		wg.Done()
	}

	numRecords := atomic.Int64{}
	tableNameRowsMapping := utils.InitialiseTableRowsMap(req.TableMappings)

	pool, err := utils.LuaPool(func() (*lua.LState, error) {
		ls, err := utils.LoadScript(wgCtx, req.Script, func(ls *lua.LState) int {
			top := ls.GetTop()
			ss := make([]string, top)
			for i := range top {
				ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
			}
			_ = c.LogFlowInfo(ctx, req.FlowJobName, strings.Join(ss, "\t"))
			return 0
		})
		if err != nil {
			return nil, err
		}
		if req.Script == "" {
			ls.Env.RawSetString("onRecord", ls.NewFunction(utils.DefaultOnRecord))
		}
		return ls, nil
	}, func(krs []*kgo.Record) {
		wg.Add(len(krs))
		for _, kr := range krs {
			c.client.Produce(wgCtx, kr, produceCb)
		}
	})
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	lastSeenLSN := atomic.Int64{}
	flushLoopDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(peerdbenv.PeerDBQueueFlushTimeoutSeconds())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-flushLoopDone:
				return
			// flush loop doesn't block processing new messages
			case <-ticker.C:
				lastSeen := lastSeenLSN.Load()
				if err := c.client.Flush(ctx); err != nil {
					c.logger.Warn("[kafka] flush error", slog.Any("error", err))
					continue
				} else if lastSeen > req.ConsumedOffset.Load() {
					if err := c.SetLastOffset(ctx, req.FlowJobName, lastSeen); err != nil {
						c.logger.Warn("[kafka] SetLastOffset error", slog.Any("error", err))
					} else {
						shared.AtomicInt64Max(req.ConsumedOffset, lastSeen)
						c.logger.Info("processBatch", slog.Int64("updated last offset", lastSeen))
					}
				}
			}
		}
	}()

Loop:
	for {
		select {
		case record, ok := <-req.Records.GetRecords():
			if !ok {
				c.logger.Info("flushing batches because no more records")
				break Loop
			}

			pool.Run(func(ls *lua.LState) []*kgo.Record {
				lfn := ls.Env.RawGetString("onRecord")
				fn, ok := lfn.(*lua.LFunction)
				if !ok {
					wgErr(fmt.Errorf("script should define `onRecord` as function, not %s", lfn))
					return nil
				}

				ls.Push(fn)
				ls.Push(pua.LuaRecord.New(ls, record))
				err := ls.PCall(1, -1, nil)
				if err != nil {
					wgErr(fmt.Errorf("script failed: %w", err))
					return nil
				}

				args := ls.GetTop()
				results := make([]*kgo.Record, 0, args)
				for i := range args {
					kr, err := lvalueToKafkaRecord(ls, ls.Get(i-args))
					if err != nil {
						wgErr(err)
						return nil
					}
					if kr != nil {
						if kr.Topic == "" {
							kr.Topic = record.GetDestinationTableName()
						}
						results = append(results, kr)
						record.PopulateCountMap(tableNameRowsMapping)
					}
				}
				ls.SetTop(0)
				numRecords.Add(1)
				shared.AtomicInt64Max(&lastSeenLSN, record.GetCheckpointID())
				return results
			})

		case <-wgCtx.Done():
			break Loop
		}
	}

	close(flushLoopDone)
	if err := pool.Wait(wgCtx); err != nil {
		return nil, err
	}
	if err := c.client.Flush(wgCtx); err != nil {
		return nil, fmt.Errorf("[kafka] final flush error: %w", err)
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
	if err := c.FinishBatch(ctx, req.FlowJobName, req.SyncBatchID, lastCheckpoint); err != nil {
		return nil, err
	}

	return &model.SyncResponse{
		CurrentSyncBatchID:     req.SyncBatchID,
		LastSyncedCheckpointID: lastCheckpoint,
		NumRecordsSynced:       numRecords.Load(),
		TableNameRowsMapping:   tableNameRowsMapping,
		TableSchemaDeltas:      req.Records.SchemaDeltas,
	}, nil
}
