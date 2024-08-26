package connpubsub

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/pua"
)

func (*PubSubConnector) SetupQRepMetadataTables(_ context.Context, _ *protos.QRepConfig) error {
	return nil
}

func (c *PubSubConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()
	numRecords := atomic.Int64{}
	schema := stream.Schema()
	topiccache := topicCache{cache: make(map[string]*pubsub.Topic)}
	publish := make(chan publishResult, 32)
	waitChan := make(chan struct{})

	queueCtx, queueErr := context.WithCancelCause(ctx)
	pool, err := c.createPool(queueCtx, config.Env, config.Script, config.FlowJobName, &topiccache, publish, queueErr)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	go func() {
		for curpub := range publish {
			if curpub.PublishResult != nil {
				if _, err := curpub.Get(ctx); err != nil {
					queueErr(err)
					break
				}
			}
		}
		close(waitChan)
	}()

Loop:
	for {
		select {
		case qrecord, ok := <-stream.Records:
			if !ok {
				c.logger.Info("flushing batches because no more records")
				break Loop
			}

			pool.Run(func(ls *lua.LState) poolResult {
				items := model.NewRecordItems(len(qrecord))
				for i, val := range qrecord {
					items.AddColumn(schema.Fields[i].Name, val)
				}
				record := &model.InsertRecord[model.RecordItems]{
					BaseRecord:           model.BaseRecord{},
					Items:                items,
					SourceTableName:      config.WatermarkTable,
					DestinationTableName: config.DestinationTableIdentifier,
					CommitID:             0,
				}

				lfn := ls.Env.RawGetString("onRecord")
				fn, ok := lfn.(*lua.LFunction)
				if !ok {
					queueErr(fmt.Errorf("script should define `onRecord` as function, not %s", lfn))
					return poolResult{}
				}

				ls.Push(fn)
				ls.Push(pua.LuaRecord.New(ls, record))
				err := ls.PCall(1, -1, nil)
				if err != nil {
					queueErr(fmt.Errorf("script failed: %w", err))
					return poolResult{}
				}

				args := ls.GetTop()
				results := make([]PubSubMessage, 0, args)
				for i := range args {
					msg, err := lvalueToPubSubMessage(ls, ls.Get(i-args))
					if err != nil {
						queueErr(err)
						return poolResult{}
					}
					if msg.Message != nil {
						if msg.Topic == "" {
							msg.Topic = record.GetDestinationTableName()
						}
						results = append(results, msg)
					}
				}
				ls.SetTop(0)
				numRecords.Add(1)
				return poolResult{messages: results}
			})

		case <-queueCtx.Done():
			break Loop
		}
	}

	if err := pool.Wait(queueCtx); err != nil {
		return 0, err
	}
	close(publish)
	topiccache.Stop(queueCtx)
	select {
	case <-queueCtx.Done():
		return 0, queueCtx.Err()
	case <-waitChan:
	}

	if err := c.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		return 0, err
	}
	return int(numRecords.Load()), nil
}
