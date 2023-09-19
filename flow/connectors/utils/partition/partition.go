package utils

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Function to compare two values
func compareValues(prevEnd interface{}, start interface{}) int {
	switch v := start.(type) {
	case int64:
		if prevEnd.(int64) < v {
			return -1
		} else if prevEnd.(int64) > v {
			return 1
		} else {
			return 0
		}
	case int32:
		if prevEnd.(int64) < int64(v) {
			return -1
		} else if prevEnd.(int64) > int64(v) {
			return 1
		} else {
			return 0
		}
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
		if pe.BlockNumber < v.BlockNumber {
			return -1
		} else if pe.BlockNumber > v.BlockNumber {
			return 1
		} else {
			if pe.OffsetNumber < v.OffsetNumber {
				return -1
			} else if pe.OffsetNumber > v.OffsetNumber {
				return 1
			} else {
				return 0
			}
		}
	default:
		return 0
	}
}

// Function to adjust start value
func adjustStartValue(prevEnd interface{}, start interface{}) interface{} {
	switch start.(type) {
	case int64:
		return prevEnd.(int64) + 1
	case int32:
		return int32(prevEnd.(int64) + 1)
	case time.Time:
		// postgres timestamp has microsecond precision
		return prevEnd.(time.Time).Add(1 * time.Microsecond)
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

func createXMINPartition(start uint32, end uint32) *protos.QRepPartition {
	return &protos.QRepPartition{
		PartitionId: uuid.New().String(),
		Range: &protos.PartitionRange{
			Range: &protos.PartitionRange_XminRange{
				XminRange: &protos.XMINPartitionRange{
					Start: start,
					End:   end,
				},
			},
		},
	}
}

type PartitionHelper struct {
	prevStart  interface{}
	prevEnd    interface{}
	partitions []*protos.QRepPartition
}

func NewPartitionHelper() *PartitionHelper {
	return &PartitionHelper{
		partitions: make([]*protos.QRepPartition, 0),
	}
}

func (p *PartitionHelper) AddPartition(start interface{}, end interface{}) error {
	log.Debugf("adding partition - start: %v, end: %v", start, end)

	// Skip partition if it's fully contained within the previous one
	// If it's not fully contained but overlaps, adjust the start
	if p.prevEnd != nil {
		comparison := compareValues(p.prevEnd, start)
		if comparison >= 0 {
			// If end is also less than or equal to prevEnd, skip this partition
			if compareValues(p.prevEnd, end) >= 0 {
				// log the skipped partition
				log.Debugf("skipping partition - start: %v, end: %v", start, end)
				log.Debugf("fully contained within previous partition: start: %v, end: %v", p.prevStart, p.prevEnd)
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
	case pgtype.Uint32:
		p.partitions = append(p.partitions, createXMINPartition(v.Uint32, end.(uint32)))
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
