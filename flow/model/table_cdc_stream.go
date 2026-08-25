package model

import (
	"context"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// TableCDCStream is a single-table records stream used by the isolated
// per-table CDC path (TableCDCPullConnector).
type TableCDCStream struct {
	records      chan Record[RecordItems]
	SchemaDeltas []*protos.TableSchemaDelta
}

func NewTableCDCStream(channelBuffer int) *TableCDCStream {
	return &TableCDCStream{
		records:      make(chan Record[RecordItems], channelBuffer),
		SchemaDeltas: make([]*protos.TableSchemaDelta, 0),
	}
}

func (r *TableCDCStream) AddRecord(ctx context.Context, record Record[RecordItems]) error {
	// hot-path optimization: avoid setting up logger/ticker unless channel is actually full
	select {
	case r.records <- record:
		return nil
	default:
	}

	logger := internal.LoggerFromCtx(ctx)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case r.records <- record:
			return nil
		case <-ticker.C:
			logger.Warn("waiting on adding record to table stream", slog.String("dstTableName", record.GetDestinationTableName()))
		case <-ctx.Done():
			logger.Warn("context cancelled while adding record to table stream", slog.String("dstTableName", record.GetDestinationTableName()))
			return ctx.Err()
		}
	}
}

func (r *TableCDCStream) AddSchemaDelta(delta *protos.TableSchemaDelta) {
	r.SchemaDeltas = append(r.SchemaDeltas, delta)
}

func (r *TableCDCStream) GetRecords() <-chan Record[RecordItems] {
	return r.records
}

func (r *TableCDCStream) Close() {
	close(r.records)
}
