package connkafka

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/pua"
)

func (*KafkaConnector) SetupQRepMetadataTables(_ context.Context, _ *protos.QRepConfig) error {
	return nil
}

func (c *KafkaConnector) SyncQRepRecords(
	ctx context.Context,
	config *protos.QRepConfig,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int, error) {
	startTime := time.Now()
	schema := stream.Schema()

	wgCtx, wgErr := context.WithCancelCause(ctx)
	produceCb := func(_ *kgo.Record, err error) {
		if err != nil {
			wgErr(err)
		}
	}

	pool, err := utils.LuaPool(func() (*lua.LState, error) {
		ls, err := utils.LoadScript(wgCtx, config.Script, func(ls *lua.LState) int {
			top := ls.GetTop()
			ss := make([]string, top)
			for i := range top {
				ss[i] = ls.ToStringMeta(ls.Get(i + 1)).String()
			}
			_ = c.LogFlowInfo(ctx, config.FlowJobName, strings.Join(ss, "\t"))
			return 0
		})
		if err != nil {
			return nil, err
		}
		if config.Script == "" {
			ls.Env.RawSetString("onRecord", ls.NewFunction(utils.DefaultOnRecord))
		}
		return ls, nil
	}, func(krs []*kgo.Record) {
		for _, kr := range krs {
			c.client.Produce(wgCtx, kr, produceCb)
		}
	})
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	numRecords := atomic.Int64{}
Loop:
	for {
		select {
		case qrecord, ok := <-stream.Records:
			if !ok {
				c.logger.Info("flushing batches because no more records")
				break Loop
			}

			pool.Run(func(ls *lua.LState) []*kgo.Record {
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
					}
				}
				ls.SetTop(0)
				numRecords.Add(1)
				return results
			})

		case <-wgCtx.Done():
			break Loop
		}
	}

	if err := pool.Wait(wgCtx); err != nil {
		return 0, err
	}
	if err := c.client.Flush(wgCtx); err != nil {
		return 0, fmt.Errorf("[kafka] final flush error: %w", err)
	}

	if err := c.FinishQRepPartition(ctx, partition, config.FlowJobName, startTime); err != nil {
		return 0, err
	}
	return int(numRecords.Load()), nil
}
