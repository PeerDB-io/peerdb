package utils

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// QRep partition constants
const (
	MySQLFullTablePartitionID = "mysql-full-table-partition-id"
	MongoFullTablePartitionID = "mongo-full-table-partition-id"
)

type PartitionRangeType string

const (
	PartitionEndRangeType   PartitionRangeType = "end"
	PartitionStartRangeType PartitionRangeType = "start"
)

type PartitionRangeForComparison struct {
	partitionRange     *protos.PartitionRange
	rangeTypeToCompare PartitionRangeType
}

// Function to adjust start value
func adjustStartValueOfPartition(prevRange *protos.PartitionRange, currentRange *protos.PartitionRange) {
	if prevRange == nil || currentRange == nil {
		return
	}

	switch cr := currentRange.Range.(type) {
	case *protos.PartitionRange_IntRange:
		if pr, ok := prevRange.Range.(*protos.PartitionRange_IntRange); ok {
			cr.IntRange.Start = pr.IntRange.End + 1
		}
		return

	case *protos.PartitionRange_UintRange:
		if pr, ok := prevRange.Range.(*protos.PartitionRange_UintRange); ok {
			cr.UintRange.Start = pr.UintRange.End + 1
		}
		return

	case *protos.PartitionRange_TimestampRange:
		if pr, ok := prevRange.Range.(*protos.PartitionRange_TimestampRange); ok {
			cr.TimestampRange.Start = timestamppb.New(pr.TimestampRange.End.AsTime().Add(1 * time.Microsecond))
		}
		return

	case *protos.PartitionRange_TidRange:
		if pr, ok := prevRange.Range.(*protos.PartitionRange_TidRange); ok {
			start := &protos.TID{
				BlockNumber:  pr.TidRange.End.BlockNumber,
				OffsetNumber: pr.TidRange.End.OffsetNumber,
			}
			if start.OffsetNumber < 0xFFFF {
				start.OffsetNumber++
			} else {
				start.BlockNumber++
				start.OffsetNumber = 0
			}
			cr.TidRange.Start = start
		}
		return

	default:
		return
	}
}

func createIntPartition(start int64, end int64) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.NewString(),
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
		PartitionId: uuid.NewString(),
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
		PartitionId: uuid.NewString(),
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
		PartitionId: uuid.NewString(),
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

// Function to compare the end of a partition with the start of another
func (p *PartitionHelper) comparePartitionRanges(
	previousPartition PartitionRangeForComparison,
	currentPartition PartitionRangeForComparison,
) int {
	if previousPartition.partitionRange == nil || currentPartition.partitionRange == nil {
		p.logger.Warn("one of the partition ranges is nil, cannot compare")
		return 0
	}
	switch pr := previousPartition.partitionRange.Range.(type) {
	case *protos.PartitionRange_IntRange:
		cr, ok := currentPartition.partitionRange.Range.(*protos.PartitionRange_IntRange)
		if !ok {
			return 0
		}
		getVal := func(r *protos.IntPartitionRange, t PartitionRangeType) int64 {
			if t == PartitionEndRangeType {
				return r.End
			}
			return r.Start
		}
		prevVal := getVal(pr.IntRange, previousPartition.rangeTypeToCompare)
		currVal := getVal(cr.IntRange, currentPartition.rangeTypeToCompare)
		return cmp.Compare(prevVal, currVal)
	case *protos.PartitionRange_UintRange:
		cr, ok := currentPartition.partitionRange.Range.(*protos.PartitionRange_UintRange)
		if !ok {
			return 0
		}
		getVal := func(r *protos.UIntPartitionRange, t PartitionRangeType) uint64 {
			if t == PartitionEndRangeType {
				return r.End
			}
			return r.Start
		}
		prevVal := getVal(pr.UintRange, previousPartition.rangeTypeToCompare)
		currVal := getVal(cr.UintRange, currentPartition.rangeTypeToCompare)
		return cmp.Compare(prevVal, currVal)
	case *protos.PartitionRange_TimestampRange:
		cr, ok := currentPartition.partitionRange.Range.(*protos.PartitionRange_TimestampRange)
		if !ok {
			return 0
		}
		getTime := func(r *protos.TimestampPartitionRange, t PartitionRangeType) time.Time {
			if t == PartitionEndRangeType {
				return r.End.AsTime()
			}
			return r.Start.AsTime()
		}
		prevVal := getTime(pr.TimestampRange, previousPartition.rangeTypeToCompare)
		currVal := getTime(cr.TimestampRange, currentPartition.rangeTypeToCompare)
		return prevVal.Compare(currVal)
	case *protos.PartitionRange_TidRange:
		cr, ok := currentPartition.partitionRange.Range.(*protos.PartitionRange_TidRange)
		if !ok {
			return 0
		}
		getTuple := func(r *protos.TIDPartitionRange, t PartitionRangeType) *protos.TID {
			if t == PartitionEndRangeType {
				return r.End
			}
			return r.Start
		}
		prevTuple := getTuple(pr.TidRange, previousPartition.rangeTypeToCompare)
		currTuple := getTuple(cr.TidRange, currentPartition.rangeTypeToCompare)
		if c := cmp.Compare(prevTuple.BlockNumber, currTuple.BlockNumber); c != 0 {
			return c
		}
		return cmp.Compare(prevTuple.OffsetNumber, currTuple.OffsetNumber)
	default:
		return 0
	}
}

func (p *PartitionHelper) AddPartition(start any, end any) error {
	p.logger.Info("adding partition", slog.Any("start", start), slog.Any("end", end))
	currentPartition, err := p.getPartitionForStartAndEnd(start, end)
	if err != nil {
		return fmt.Errorf("error getting current partition from start and end: %w", err)
	}
	if currentPartition == nil {
		// should only happen when partition column entirely nil, okay to ignore initial load in this case
		p.logger.Warn("null partition, skipping", slog.Any("start", start), slog.Any("end", end))
		return nil
	}

	prevPartition, err := p.getPartitionForStartAndEnd(p.prevStart, p.prevEnd)
	if err != nil {
		return fmt.Errorf("error getting previous partition from prevStart and prevEnd: %w", err)
	}

	// Skip partition if it's fully contained within the previous one
	// If it's not fully contained but overlaps, adjust the start
	if prevPartition != nil {
		prevEndCompareStart := p.comparePartitionRanges(
			PartitionRangeForComparison{
				partitionRange:     prevPartition.Range,
				rangeTypeToCompare: PartitionEndRangeType,
			},
			PartitionRangeForComparison{
				partitionRange:     currentPartition.Range,
				rangeTypeToCompare: PartitionStartRangeType,
			})
		if prevEndCompareStart >= 0 {
			prevEndCompareEnd := p.comparePartitionRanges(
				PartitionRangeForComparison{
					partitionRange:     prevPartition.Range,
					rangeTypeToCompare: PartitionEndRangeType,
				},
				PartitionRangeForComparison{
					partitionRange:     currentPartition.Range,
					rangeTypeToCompare: PartitionEndRangeType,
				},
			)
			// If end is also less than or equal to prevEnd, skip this partition
			if prevEndCompareEnd >= 0 {
				// log the skipped partition
				p.logger.Info("skipping partition, fully contained within previous partition",
					slog.Any("start", start), slog.Any("end", end), slog.Any("prevStart", p.prevStart), slog.Any("prevEnd", p.prevEnd))
				return nil
			}
			// If end is greater than prevEnd, adjust the start
			adjustStartValueOfPartition(prevPartition.Range, currentPartition.Range)
		}
	}

	if err := p.updatePartitionHelper(currentPartition); err != nil {
		return fmt.Errorf("error adjusting start value: %w", err)
	}

	return nil
}

func (p *PartitionHelper) AddPartitionsWithRange(start any, end any, numPartitions int64) error {
	partition, err := p.getPartitionForStartAndEnd(start, end)
	if err != nil {
		return err
	} else if partition == nil {
		p.logger.Warn("null partition range, skipping", slog.Any("start", start), slog.Any("end", end))
		return nil
	}

	switch r := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		size := shared.DivCeil(r.IntRange.End-r.IntRange.Start, numPartitions)
		for i := range numPartitions {
			if err := p.AddPartition(r.IntRange.Start+size*i, min(r.IntRange.Start+size*(i+1), r.IntRange.End)); err != nil {
				return err
			}
		}
	case *protos.PartitionRange_UintRange:
		size := shared.DivCeil(r.UintRange.End-r.UintRange.Start, uint64(numPartitions))
		for i := range uint64(numPartitions) {
			if err := p.AddPartition(r.UintRange.Start+size*i, min(r.UintRange.Start+size*(i+1), r.UintRange.End)); err != nil {
				return err
			}
		}
	case *protos.PartitionRange_TimestampRange:
		tstart := r.TimestampRange.Start.AsTime().UnixMicro()
		tend := r.TimestampRange.End.AsTime().UnixMicro()
		size := shared.DivCeil(tend-tstart, numPartitions)
		for i := range numPartitions {
			if err := p.AddPartition(time.UnixMicro(tstart+size*i), time.UnixMicro(min(tstart+size*(i+1), tend))); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *PartitionHelper) getPartitionForStartAndEnd(start any, end any) (*protos.QRepPartition, error) {
	if start == nil || end == nil {
		return nil, nil
	}
	switch v := start.(type) {
	case int64:
		return createIntPartition(v, end.(int64)), nil
	case uint64:
		return createUIntPartition(v, end.(uint64)), nil
	case int32:
		return createIntPartition(int64(v), int64(end.(int32))), nil
	case uint32:
		return createUIntPartition(uint64(v), uint64(end.(uint32))), nil
	case int16:
		return createIntPartition(int64(v), int64(end.(int16))), nil
	case uint16:
		return createUIntPartition(uint64(v), uint64(end.(uint16))), nil
	case int8:
		return createIntPartition(int64(v), int64(end.(int8))), nil
	case uint8:
		return createUIntPartition(uint64(v), uint64(end.(uint8))), nil
	case time.Time:
		return createTimePartition(v, end.(time.Time)), nil
	case pgtype.TID:
		return createTIDPartition(v, end.(pgtype.TID)), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

func (p *PartitionHelper) updatePartitionHelper(partition *protos.QRepPartition) error {
	if partition == nil {
		return errors.New("partition is nil")
	}
	p.partitions = append(p.partitions, partition)

	switch r := partition.Range.Range.(type) {
	case *protos.PartitionRange_IntRange:
		p.prevStart = r.IntRange.Start
		p.prevEnd = r.IntRange.End
	case *protos.PartitionRange_UintRange:
		p.prevStart = r.UintRange.Start
		p.prevEnd = r.UintRange.End
	case *protos.PartitionRange_TimestampRange:
		p.prevStart = r.TimestampRange.Start.AsTime()
		p.prevEnd = r.TimestampRange.End.AsTime()
	case *protos.PartitionRange_TidRange:
		p.prevStart = pgtype.TID{
			BlockNumber:  r.TidRange.Start.BlockNumber,
			OffsetNumber: uint16(r.TidRange.Start.OffsetNumber),
		}
		p.prevEnd = pgtype.TID{
			BlockNumber:  r.TidRange.End.BlockNumber,
			OffsetNumber: uint16(r.TidRange.End.OffsetNumber),
		}
	default:
		return fmt.Errorf("unsupported partition range type: %T", r)
	}

	return nil
}

func (p *PartitionHelper) GetPartitions() []*protos.QRepPartition {
	return p.partitions
}
