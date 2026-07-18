package connmysql

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

var (
	uuidLowerRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uuidUpperRe = regexp.MustCompile(`^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$`)
)

type hexCasing int

const (
	hexCasingUnknown hexCasing = iota
	hexCasingLower
	hexCasingUpper
)

// detectUuidWithHexCasing best-effort classifies a string watermark column by
// inspecting only its min and max bounds: it determines whether both are
// canonical UUIDs, and if so, the shared casing of their hex letters.
//
// Because only the bounds are examined, the classification can be wrong in
// the following rare cases:
//   - non-UUID rows can exist between UUID-shaped min/max bounds;
//   - non-boundary UUIDs contain a mix of upper and lower case
//   - min/max bounds both don't contain hex, so casing blindly defaults to lower
//
// These cases may lead to partition skew, but will not affect correctness.
func detectUuidWithHexCasing(minVal string, maxVal string) (bool, hexCasing) {
	switch {
	case uuidLowerRe.MatchString(minVal) && uuidLowerRe.MatchString(maxVal):
		return true, hexCasingLower
	case uuidUpperRe.MatchString(minVal) && uuidUpperRe.MatchString(maxVal):
		return true, hexCasingUpper
	default:
		return false, hexCasingUnknown
	}
}

// buildUuidStringPartitions splits a UUID string watermark column into partitions.
// The original minVal/maxVal are used for first/last partitions to ensure correctness.
func buildUuidStringPartitions(
	minVal string, maxVal string, casing hexCasing, numPartitions int64,
) ([]*protos.QRepPartition, error) {
	minInt, err := uuidToBigInt(minVal)
	if err != nil {
		return nil, fmt.Errorf("failed to convert min uuid to bigint: %w", err)
	}
	maxInt, err := uuidToBigInt(maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed to convert max uuid to bigint: %w", err)
	}
	if minInt.Cmp(maxInt) > 0 {
		return nil, errors.New("min uuid greater than max uuid")
	}

	var partitions []*protos.QRepPartition
	start := minVal
	step := shared.BigIntDivCeil(new(big.Int).Sub(maxInt, minInt), big.NewInt(numPartitions))
	for value := new(big.Int).Add(minInt, step); value.Cmp(maxInt) < 0; value.Add(value, step) {
		end, err := bigIntToUUID(value, casing)
		if err != nil {
			return nil, fmt.Errorf("failed to convert bigint to uuid: %w", err)
		}
		partitions = append(partitions, utils.CreateStringPartition(start, end, false))
		start = end
	}
	partitions = append(partitions, utils.CreateStringPartition(start, maxVal, true))
	return partitions, nil
}

func uuidToBigInt(s string) (*big.Int, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(u[:]), nil
}

func bigIntToUUID(n *big.Int, casing hexCasing) (string, error) {
	if n.BitLen() > 128 {
		return "", fmt.Errorf("value does not fit in a UUID (%d bits)", n.BitLen())
	}
	var b [16]byte
	n.FillBytes(b[:])
	s := uuid.UUID(b).String()
	if casing == hexCasingUpper {
		s = strings.ToUpper(s)
	}
	return s, nil
}

const (
	base95Min   = ' ' // 0x20, lowest printable ASCII -> digit 0
	base95Max   = '~' // 0x7E, highest printable ASCII -> digit 94
	base95Radix = base95Max - base95Min + 1
	base95Width = 8 // 95^8 to fit in an uint64
)

func stringMidpoint(s1 string, s2 string) string {
	i := 0
	for i < len(s1) && i < len(s2) && s1[i] == s2[i] {
		i++
	}
	// This can break multibyte characters in the middle but the queries still
	// succeed and the whole range is covered, even if with a skew
	sharedPrefix := s1[:i]
	s1, s2 = s1[i:], s2[i:]
	mid := (stringToBase95Integer(s1) + stringToBase95Integer(s2)) / 2
	return strings.TrimRight(sharedPrefix+base95IntegerToString(mid), " ")
}

func stringToBase95Integer(s string) uint64 {
	if s == "" {
		return 0
	}
	var res uint64
	for i := range base95Width {
		var digit uint64
		if i < len(s) {
			ch := s[i]
			switch {
			case ch < base95Min:
				ch = base95Min
			case ch > base95Max:
				ch = base95Max
			}
			digit = uint64(ch - base95Min)
		}
		res = res*base95Radix + digit
	}
	return res
}

func base95IntegerToString(n uint64) string {
	digits := make([]byte, base95Width)
	for k := base95Width - 1; k >= 0; k-- {
		digits[k] = base95Min + byte(n%base95Radix)
		n /= base95Radix
	}
	return string(digits)
}

type stringPartitionEntry struct {
	start string
	end   string
	rows  uint64
}

type stringPartitionHeap []stringPartitionEntry

func (h *stringPartitionHeap) Len() int           { return len(*h) }
func (h *stringPartitionHeap) Less(i, j int) bool { return (*h)[i].rows > (*h)[j].rows }
func (h *stringPartitionHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *stringPartitionHeap) Push(x any) { *h = append(*h, x.(stringPartitionEntry)) }

func (h *stringPartitionHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// interface for unit-testing
type rangeProber interface {
	estimateRowsInRange(ctx context.Context, tableName string, quotedCol string, start string, end string) (uint64, error)
	fetchNextRealKey(
		ctx context.Context, tableName string, quotedCol string, midpoint string, start string, end string,
	) (string, bool, error)
	fetchPrevRealKey(
		ctx context.Context, tableName string, quotedCol string, midpoint string, start string, end string,
	) (string, bool, error)
}

// buildAdaptiveStringPartitions splits an arbitrary string watermark column
// into at most numPartitions partitions using midpoint bisection guided
// by the query planner's row estimates. It starts from a single [minVal, maxVal]
// partition and repeatedly splits the largest partition, until it reaches
// numPartitions or runs out of splittable partitions.
func buildAdaptiveStringPartitions(
	ctx context.Context,
	prober rangeProber,
	logger log.Logger,
	table *common.QualifiedTable,
	watermarkColumn string,
	minVal string,
	maxVal string,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	tableName := table.MySQL()
	quotedCol := common.QuoteMySQLIdentifier(watermarkColumn)

	totalRows, err := prober.estimateRowsInRange(ctx, tableName, quotedCol, minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed to estimate rows: %w", err)
	}
	h := &stringPartitionHeap{{start: minVal, end: maxVal, rows: totalRows}}
	heap.Init(h)

	var outputs []stringPartitionEntry
	for int64(len(outputs)+h.Len()) < numPartitions && h.Len() > 0 {
		p := heap.Pop(h).(stringPartitionEntry)

		if p.start == p.end {
			outputs = append(outputs, p)
			continue
		}

		mid := stringMidpoint(p.start, p.end)
		k, found, err := prober.fetchNextRealKey(ctx, tableName, quotedCol, mid, p.start, p.end)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch next real key: %w", err)
		}
		if !found {
			// The interpolated midpoint can overshoot every key in the range when
			// keys occupy a narrow slice of the character space. So also probe
			// backwards before declaring the partition unsplittable.
			k, found, err = prober.fetchPrevRealKey(ctx, tableName, quotedCol, mid, p.start, p.end)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch prev real key: %w", err)
			}
		}
		if !found {
			outputs = append(outputs, p)
			continue
		}

		leftRows, err := prober.estimateRowsInRange(ctx, tableName, quotedCol, p.start, k)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate rows: %w", err)
		}
		rightRows, err := prober.estimateRowsInRange(ctx, tableName, quotedCol, k, p.end)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate rows: %w", err)
		}
		heap.Push(h, stringPartitionEntry{start: p.start, end: k, rows: leftRows})
		heap.Push(h, stringPartitionEntry{start: k, end: p.end, rows: rightRows})
	}

	for h.Len() > 0 {
		outputs = append(outputs, heap.Pop(h).(stringPartitionEntry))
	}

	partitions := make([]*protos.QRepPartition, 0, len(outputs))
	for _, p := range outputs {
		partitions = append(partitions, utils.CreateStringPartition(p.start, p.end, p.end == maxVal))
	}
	logger.Info("[mysql] built adaptive string partitions",
		slog.Int64("targetNumPartitions", numPartitions),
		slog.Int("numPartitions", len(partitions)))

	return partitions, nil
}

// fetchNextRealKey returns the smallest real value of the watermark column that
// is at or past the interpolated midpoint and strictly inside (start, end). The
// bounds are enforced by MySQL using its column's collation. Return false when
// no such key exists.
func (c *MySqlConnector) fetchNextRealKey(
	ctx context.Context, tableName string, quotedCol string, midpoint string, start string, end string,
) (string, bool, error) {
	query := fmt.Sprintf(
		"SELECT %[1]s FROM %[2]s WHERE %[1]s >= '%[3]s' AND %[1]s > '%[4]s' AND %[1]s < '%[5]s' ORDER BY %[1]s LIMIT 1",
		quotedCol, tableName, escapeWithNoBackslashEscapes(midpoint), escapeWithNoBackslashEscapes(start), escapeWithNoBackslashEscapes(end))
	return c.fetchRealKey(ctx, query)
}

// fetchPrevRealKey returns the largest real value of the watermark column that
// is below the interpolated midpoint and strictly inside (start, end). The
// bounds are enforced by MySQL using its column's collation. Return false when
// no such key exists.
func (c *MySqlConnector) fetchPrevRealKey(
	ctx context.Context, tableName string, quotedCol string, midpoint string, start string, end string,
) (string, bool, error) {
	query := fmt.Sprintf(
		"SELECT %[1]s FROM %[2]s WHERE %[1]s < '%[3]s' AND %[1]s > '%[4]s' AND %[1]s < '%[5]s' ORDER BY %[1]s DESC LIMIT 1",
		quotedCol, tableName, escapeWithNoBackslashEscapes(midpoint), escapeWithNoBackslashEscapes(start), escapeWithNoBackslashEscapes(end))
	return c.fetchRealKey(ctx, query)
}

func (c *MySqlConnector) fetchRealKey(ctx context.Context, query string) (string, bool, error) {
	rs, err := c.Execute(ctx, query)
	if err != nil {
		return "", false, err
	}
	defer rs.Close()
	if rs.RowNumber() == 0 {
		return "", false, nil
	}
	key, err := rs.GetString(0, 0)
	if err != nil {
		return "", false, fmt.Errorf("failed to read real key: %w", err)
	}
	// GetString is zero-copy over the resultset's row buffer, which rs.Close()
	// recycles into a pool where the next query's response overwrites it; the
	// key is stored in the heap and outlives the function, so it must own its bytes
	return strings.Clone(key), true, nil
}

// estimateRowsInRange asks the query planner for the estimated number of rows in
// [start, end). The estimate is best-effort: a NULL or zero estimate is returned
// as 0 rather than an error, so the caller still treats the range as a real partition.
func (c *MySqlConnector) estimateRowsInRange(
	ctx context.Context, tableName string, quotedCol string, start string, end string,
) (uint64, error) {
	query := fmt.Sprintf(
		"EXPLAIN FORMAT=TRADITIONAL SELECT 1 FROM %[1]s WHERE %[2]s >= '%[3]s' AND %[2]s < '%[4]s'",
		tableName, quotedCol, escapeWithNoBackslashEscapes(start), escapeWithNoBackslashEscapes(end))
	rs, err := c.Execute(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rs.Close()
	if rs.RowNumber() == 0 {
		return 0, fmt.Errorf("EXPLAIN returned no rows for table %s", tableName)
	}
	rows, err := rs.GetUintByName(0, "rows")
	if err != nil {
		return 0, err
	}
	return rows, nil
}
