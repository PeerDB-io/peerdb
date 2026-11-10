package common

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// MinSyncCronGap is the shortest allowed gap between two consecutive fires of a sync cron schedule.
const MinSyncCronGap = time.Minute

// ParseSyncCron parses a standard 5-field cron expression (or a descriptor such as @hourly).
// Schedules are evaluated in UTC unless the expression carries its own CRON_TZ= prefix.
func ParseSyncCron(expr string) (cron.Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, errors.New("cron expression is empty")
	}
	if !strings.HasPrefix(expr, "CRON_TZ=") && !strings.HasPrefix(expr, "TZ=") {
		expr = "CRON_TZ=UTC " + expr
	}
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	first := schedule.Next(time.Now())
	if first.IsZero() {
		return nil, errors.New("cron expression never fires")
	}
	if gap := schedule.Next(first).Sub(first); gap < MinSyncCronGap {
		return nil, fmt.Errorf("cron schedule must fire at most once per %s", MinSyncCronGap)
	}
	return schedule, nil
}
