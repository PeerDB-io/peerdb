package conncockroachdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// crdbSystemTimeRegex matches a CockroachDB HLC timestamp, e.g. 1712345678901234567.0000000001
var crdbSystemTimeRegex = regexp.MustCompile(`^\d+(\.\d+)?$`)

// crdbHLC is a decoded CockroachDB hybrid logical clock timestamp, mirroring
// the shape of CockroachDB's hlc.Timestamp (wall-clock nanoseconds plus a
// logical counter). Snapshot timestamps and changefeed cursors are handled in
// this typed form internally; strings appear only at the SQL and persistence
// boundaries. WallNanos doubles as PeerDB's int64 CDC offset.
type crdbHLC struct {
	WallNanos int64
	Logical   int64
}

// parseHLC decodes the decimal HLC text CockroachDB emits from
// cluster_logical_timestamp() and in changefeed resolved/updated fields
// ("<wallnanos>.<10-digit logical>", the logical part optional).
func parseHLC(s string) (crdbHLC, error) {
	if !crdbSystemTimeRegex.MatchString(s) {
		return crdbHLC{}, fmt.Errorf("invalid CockroachDB HLC timestamp %q", s)
	}
	wall, logical, hasLogical := strings.Cut(s, ".")
	var ts crdbHLC
	var err error
	if ts.WallNanos, err = strconv.ParseInt(wall, 10, 64); err != nil {
		return crdbHLC{}, fmt.Errorf("invalid CockroachDB HLC timestamp %q: %w", s, err)
	}
	if hasLogical {
		if ts.Logical, err = strconv.ParseInt(logical, 10, 64); err != nil {
			return crdbHLC{}, fmt.Errorf("invalid CockroachDB HLC timestamp %q: %w", s, err)
		}
	}
	return ts, nil
}

// String renders the timestamp in the decimal form CockroachDB accepts as a
// changefeed cursor and AS OF SYSTEM TIME argument.
func (ts crdbHLC) String() string {
	return fmt.Sprintf("%d.%010d", ts.WallNanos, ts.Logical)
}

// clusterLogicalTimestamp captures the current HLC timestamp of the cluster
// as a consistent point for snapshot reads and changefeed cursors.
func (c *CockroachDBConnector) clusterLogicalTimestamp(ctx context.Context) (crdbHLC, error) {
	var systemTime string
	if err := c.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()::text").Scan(&systemTime); err != nil {
		return crdbHLC{}, fmt.Errorf("failed to get cluster logical timestamp: %w", err)
	}
	return parseHLC(systemTime)
}
