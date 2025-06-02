package utils

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
func compareValues(prevEnd any, start any) (int, error) {
	switch v := start.(type) {
	case int64:
		return cmp.Compare(prevEnd.(int64), v), nil
	case uint64:
		return cmp.Compare(prevEnd.(uint64), v), nil
	case time.Time:
		return prevEnd.(time.Time).Compare(v), nil
	case pgtype.TID:
		pe := prevEnd.(pgtype.TID)
		if c := cmp.Compare(pe.BlockNumber, v.BlockNumber); c != 0 {
			return c, nil
		}
		return cmp.Compare(pe.OffsetNumber, v.OffsetNumber), nil
	case uint32: // xmin
		switch prevEnd := prevEnd.(type) {
		case uint32:
			return cmp.Compare(prevEnd, v), nil
		case uint64:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case int64:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case int32:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case int16:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case int8:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case uint16:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		case uint8:
			return cmp.Compare(uint64(prevEnd), uint64(v)), nil
		default:
			return 0, fmt.Errorf("unsupported type for uint32 comparison: %T", prevEnd)
		}
	default:
		return 0, fmt.Errorf("unsupported type for comparison: %T", start)
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
		// postgres & mysql timestamps have microsecond precision
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

func createUIntPartition(start uint64, end uint64) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.New().String(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_UintRange{
				UintRange: &protos.UIntPartitionRange{
					Start: start,
					End:   end,
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
		prevEndCompareStart, err := compareValues(p.prevEnd, start)
		if err != nil {
			return fmt.Errorf("error comparing previous end with current start: %w", err)
		}
		if prevEndCompareStart >= 0 {
			prevEndCompareEnd, err := compareValues(p.prevEnd, end)
			if err != nil {
				return fmt.Errorf("error comparing previous end with current end: %w", err)
			}
			// If end is also less than or equal to prevEnd, skip this partition
			if prevEndCompareEnd >= 0 {
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
	case uint64:
		p.partitions = append(p.partitions, createUIntPartition(v, end.(uint64)))
		p.prevStart = v
		p.prevEnd = end.(uint64)
	case int32:
		p.partitions = append(p.partitions, createIntPartition(int64(v), int64(end.(int32))))
		p.prevStart = int64(v)
		p.prevEnd = int64(end.(int32))
	case uint32:
		p.partitions = append(p.partitions, createUIntPartition(uint64(v), uint64(end.(uint32))))
		p.prevStart = uint64(v)
		p.prevEnd = uint64(end.(uint32))
	case int16:
		p.partitions = append(p.partitions, createIntPartition(int64(v), int64(end.(int16))))
		p.prevStart = int64(v)
		p.prevEnd = int64(end.(int16))
	case uint16:
		p.partitions = append(p.partitions, createUIntPartition(uint64(v), uint64(end.(uint16))))
		p.prevStart = uint64(v)
		p.prevEnd = uint64(end.(uint16))
	case int8:
		p.partitions = append(p.partitions, createIntPartition(int64(v), int64(end.(int8))))
		p.prevStart = int64(v)
		p.prevEnd = int64(end.(int8))
	case uint8:
		p.partitions = append(p.partitions, createUIntPartition(uint64(v), uint64(end.(uint8))))
		p.prevStart = uint64(v)
		p.prevEnd = uint64(end.(uint8))
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
