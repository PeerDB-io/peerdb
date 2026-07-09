package connmysql

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
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
		return nil, fmt.Errorf("min uuid (%s) greater than max uuid (%s)", minVal, maxVal)
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
