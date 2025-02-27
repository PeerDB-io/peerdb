package partition_utils

import (
	"cmp"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Function to compare two values
func compareValues(prevEnd any, start any) int {
	switch v := start.(type) {
	case int64:
		return cmp.Compare(prevEnd.(int64), v)
	case int32:
		return cmp.Compare(prevEnd.(int64), int64(v))
	case time.Time:
		if prevEnd.(time.Time).Before(v) {
			return -1
		} else if prevEnd.(time.Time).After(v) {
			return 1
		} else {
			return 0
		}
	case pgtype.TID:
		pe := prevEnd.(pgtype.TID)
		if c := cmp.Compare(pe.BlockNumber, v.BlockNumber); c != 0 {
			return c
		}
		return cmp.Compare(pe.OffsetNumber, v.OffsetNumber)
	case uint32: // xmin
		return cmp.Compare(prevEnd.(uint32), v)
	default:
		return 0
	}
}

// Function to adjust start value
func adjustStartValue(prevEnd any, start any) any {
	switch start.(type) {
	case int64:
		return prevEnd.(int64) + 1
	case int32:
		return int32(prevEnd.(int64) + 1)
	case time.Time:
		// postgres timestamp has microsecond precision
		return prevEnd.(time.Time).Add(1 * time.Microsecond)
	case pgtype.TID:
		pe := prevEnd.(pgtype.TID)
		if pe.OffsetNumber < 0xFFFF {
			pe.OffsetNumber++
		} else {
			pe.BlockNumber++
			pe.OffsetNumber = 0
		}
		return pe
	case uint32:
		return prevEnd.(uint32) + 1
	default:
		return start
	}
}

func createIntPartition(start int64, end int64) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.New().String(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_IntRange{
				IntRange: &protos.IntPartitionRange{
					Start: start,
					End:   end,
				},
			},
		},
	}
}

func createTimePartition(start time.Time, end time.Time) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.New().String(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_TimestampRange{
				TimestampRange: &protos.TimestampPartitionRange{
					Start: timestamppb.New(start),
					End:   timestamppb.New(end),
				},
			},
		},
	}
}

func createTIDPartition(start pgtype.TID, end pgtype.TID) *protos.QRepPartition {
	startTuple := &protos.TID{
		BlockNumber:  start.BlockNumber,
		OffsetNumber: uint32(start.OffsetNumber),
	}

	endTuple := &protos.TID{
		BlockNumber:  end.BlockNumber,
		OffsetNumber: uint32(end.OffsetNumber),
	}

	return &protos.QRepPartition{
		PartitionId: uuid.New().String(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_TidRange{
				TidRange: &protos.TIDPartitionRange{
					Start: startTuple,
					End:   endTuple,
				},
			},
		},
	}
}

type PartitionHelper struct {
	logger     log.Logger
	prevStart  any
	prevEnd    any
	partitions []*protos.QRepPartition
}

func NewPartitionHelper(logger log.Logger) *PartitionHelper {
	return &PartitionHelper{
		logger:     logger,
		partitions: make([]*protos.QRepPartition, 0),
	}
}

func (p *PartitionHelper) AddPartition(start any, end any) error {
	p.logger.Info("adding partition", slog.Any("start", start), slog.Any("end", end))

	// Skip partition if it's fully contained within the previous one
	// If it's not fully contained but overlaps, adjust the start
	if p.prevEnd != nil {
		comparison := compareValues(p.prevEnd, start)
		if comparison >= 0 {
			// If end is also less than or equal to prevEnd, skip this partition
			if compareValues(p.prevEnd, end) >= 0 {
				// log the skipped partition
				p.logger.Info("skipping partition, fully contained within previous partition",
					slog.Any("start", start), slog.Any("end", end), slog.Any("prevStart", p.prevStart), slog.Any("prevEnd", p.prevEnd))
				return nil
			}
			// If end is greater than prevEnd, adjust the start
			start = adjustStartValue(p.prevEnd, start)
		}
	}

	switch v := start.(type) {
	case int64:
		p.partitions = append(p.partitions, createIntPartition(v, end.(int64)))
		p.prevStart = v
		p.prevEnd = end
	case int32:
		p.partitions = append(p.partitions, createIntPartition(int64(v), int64(end.(int32))))
		p.prevStart = int64(v)
		p.prevEnd = int64(end.(int32))
	case time.Time:
		p.partitions = append(p.partitions, createTimePartition(v, end.(time.Time)))
		p.prevStart = v
		p.prevEnd = end
	case pgtype.TID:
		p.partitions = append(p.partitions, createTIDPartition(v, end.(pgtype.TID)))
		p.prevStart = v
		p.prevEnd = end
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}

	return nil
}

func (p *PartitionHelper) GetPartitions() []*protos.QRepPartition {
	return p.partitions
}
