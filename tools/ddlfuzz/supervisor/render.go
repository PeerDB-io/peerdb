package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	sgrReset   = "\x1b[0m"
	sgrBold    = "\x1b[1m"
	sgrDim     = "\x1b[2m"
	sgrRed     = "\x1b[31m"
	sgrGreen   = "\x1b[32m"
	sgrYellow  = "\x1b[33m"
	sgrMagenta = "\x1b[35m"
	sgrCyan    = "\x1b[36m"
)

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func RenderStatus(s StatusSnapshot, color bool, width int) string {
	if width <= 0 {
		width = 130
	}
	var lines []string
	lines = append(lines, renderHeader(s, color, width)...)
	lines = append(lines, "")
	if width >= 110 {
		col := (width - 4) / 2
		lines = append(lines, sideBySide(renderFast(s, color, col), renderE2E(s, color, col), col, 4)...)
		lines = append(lines, "")
		right := append(renderFixAgent(s, color, col), "")
		right = append(right, renderEvents(s, color, col)...)
		lines = append(lines, sideBySide(renderFindings(s, color, col), right, col, 4)...)
	} else {
		lines = append(lines, renderFast(s, color, width)...)
		lines = append(lines, renderE2E(s, color, width)...)
		lines = append(lines, renderFindings(s, color, width)...)
		lines = append(lines, renderFixAgent(s, color, width)...)
		lines = append(lines, renderEvents(s, color, width)...)
	}
	for i := range lines {
		lines[i] = truncateVisible(lines[i], width)
	}
	return strings.Join(lines, "\n") + "\n"
}

func renderHeader(s StatusSnapshot, color bool, width int) []string {
	status := "● down"
	code := sgrRed
	if s.Run.Blocked {
		status = "● BLOCKED"
	} else if s.Run.Alive {
		status = "● running"
		code = sgrGreen
	}
	right := paint(color, code, status)
	lines := []string{paneRule("ddlfuzz", right, width, color)}
	rows := [][2]string{
		{"supervisor", fmt.Sprintf("pid %s · up %s", valOrNA(s.Run.PID), sinceOrNA(s.Run.StartedAt, s.Now))},
		{"deadline", deadlineText(s.Run.Deadline, s.Now)},
		{"spend", fmt.Sprintf("%s in / %s out", fmtCount(s.Run.Spend.Tokens.Input), fmtCount(s.Run.Spend.Tokens.Output))},
		{"started", timeOrNA(s.Run.StartedAt)},
		{"disk free", formatGiB(s.Run.DiskFreeBytes)},
	}
	if !s.Run.Alive && s.Run.LastSeenAge != "" {
		rows = append(rows, [2]string{"last seen", s.Run.LastSeenAge + " ago"})
	}
	if s.Run.Blocked {
		rows = append(rows, [2]string{"blocked", s.Run.BlockedReason})
	}
	if s.Run.RestartRequired {
		rows = append(rows, [2]string{"restart", "required at " + shortSHA(s.Run.RestartHead)})
	}
	rows = append(rows, [2]string{"merge slot", s.Merge.Line})
	lines = append(lines, labelGrid(rows, 3, width, color)...)
	return lines
}

func renderFast(s StatusSnapshot, color bool, width int) []string {
	right := "stats " + s.Fast.StatsAge + " ago"
	if s.Fast.StatsStale {
		right = paint(color, sgrYellow, "stale "+s.Fast.StatsAge)
	}
	lines := []string{paneRule("FAST LANE", right, width, color)}
	dimNow := func(cell string) string {
		if s.Fast.StatsStale {
			return paint(color, sgrDim, cell)
		}
		return cell
	}
	rows := [][]string{
		{"execs/s", dimNow(fmtCount(s.Fast.Stats.ExecsPerSec)), rateString(s.Fast.ExecsRate1m, false), rateString(s.Fast.ExecsRate15m, false)},
		{"suppressed/s", dimNow(rateString(s.Fast.SuppRateNow, false)), rateString(s.Fast.SuppRate1m, false), rateString(s.Fast.SuppRate15m, false)},
		{"edges mysql", dimNow(fmtGrouped(s.Fast.Stats.Edges["mysql"])), signedOrNA(s.Fast.EdgesDelta1m, "mysql"), signedOrNA(s.Fast.EdgesDelta15m, "mysql")},
		{"edges mariadb", dimNow(fmtGrouped(s.Fast.Stats.Edges["mariadb"])), signedOrNA(s.Fast.EdgesDelta1m, "mariadb"), signedOrNA(s.Fast.EdgesDelta15m, "mariadb")},
	}
	lines = append(lines, metricTable([]string{"METRIC", "NOW", "Δ1m", "Δ15m"}, rows, width, color)...)
	lines = append(lines, labelGrid([][2]string{{"trend execs/s", s.Fast.ExecSparkline}, {"corpus", fmt.Sprintf("%s my · %s ma", fmtCount(s.Fast.Stats.CorpusCount["mysql"]), fmtCount(s.Fast.Stats.CorpusCount["mariadb"]))}, {"restarts", fmt.Sprintf("%d my · %d ma", s.Fast.Stats.OracleRestarts["mysql"], s.Fast.Stats.OracleRestarts["mariadb"])}}, 1, width, color)...)
	return lines
}

func renderE2E(s StatusSnapshot, color bool, width int) []string {
	right := "hb " + s.E2E.Age + " ago"
	if s.E2E.Stale {
		right = paint(color, sgrYellow, "stale "+s.E2E.Age)
	}
	lines := []string{paneRule("E2E LANE", right, width, color)}
	rows := [][]string{
		{"cases/min", rateString(s.E2E.CasesRateNow, true), rateString(s.E2E.CasesRate1m, true), rateString(s.E2E.CasesRate15m, true)},
		{"exec-rejects/min", rateString(s.E2E.RejectsRateNow, true), rateString(s.E2E.RejectsRate1m, true), rateString(s.E2E.RejectsRate15m, true)},
	}
	lines = append(lines, metricTable([]string{"METRIC", "NOW", "Δ1m", "Δ15m"}, rows, width, color)...)
	lines = append(lines, labelGrid([][2]string{{"cases", fmt.Sprintf("%s my · %s ma", fmtCount(s.E2E.Cases["mysql"]), fmtCount(s.E2E.Cases["mariadb"]))}, {"queue", fmt.Sprintf("%d pending · %d processing · %d done", s.E2E.Queue.Pending, s.E2E.Queue.Processing, s.E2E.Queue.Done)}, {"trend cases", s.E2E.CasesSparkline}}, 1, width, color)...)
	return lines
}

func renderFindings(s StatusSnapshot, color bool, width int) []string {
	c := s.Findings.Counts
	right := fmt.Sprintf("open %d · fixed %d · parked %d · +%d/15m", c.Open, c.Fixed, c.Parked, s.Findings.New15m)
	lines := []string{paneRule("FINDINGS", right, width, color)}
	var rows [][]string
	for _, g := range s.Findings.Groups {
		flags := g.Flags
		if width < 70 {
			flags = strings.ReplaceAll(flags, "flap-parked", "flap")
		}
		name := g.ClassShape
		if g.Parked {
			name = paint(color, sgrMagenta, name)
		}
		rows = append(rows, []string{name, strconv.Itoa(g.Sigs), strconv.Itoa(g.Attempts), g.OldestAge, fmt.Sprintf("%d/%d", g.MySQL, g.MariaDB), flags})
	}
	lines = append(lines, metricTable([]string{"CLASS|SHAPE", "SIGS", "ATT", "OLDEST", "MY/MA", "FLAGS"}, rows, width, color)...)
	if s.Findings.MoreGroups > 0 {
		lines = append(lines, "  "+paint(color, sgrDim, fmt.Sprintf("(+%d more groups)", s.Findings.MoreGroups)))
	}
	return lines
}

func renderFixAgent(s StatusSnapshot, color bool, width int) []string {
	right := "idle — " + s.FixAgent.Reason
	if s.FixAgent.Attempt != nil {
		a := s.FixAgent.Attempt
		right = fmt.Sprintf("● attempt %d/%d · %s/%s", a.Attempt, a.MaxAttempts, s.FixAgent.Elapsed, s.FixAgent.Budget)
		code := sgrGreen
		elapsed := s.Now.Sub(a.StartedAt)
		if elapsed > 40*time.Minute {
			code = sgrRed
		} else if elapsed > 30*time.Minute || s.FixAgent.Stale {
			code = sgrYellow
		}
		if s.FixAgent.Stale {
			right = "stale (killed mid-attempt?)"
		}
		right = paint(color, code, right)
	}
	lines := []string{paneRule("FIX AGENT", right, width, color)}
	var rows [][2]string
	if a := s.FixAgent.Attempt; a != nil {
		rows = append(rows, [2]string{"working on", fmt.Sprintf("%s (%s|%s, %s) · phase %s", a.Sig, a.Class, a.Shape, a.Engine, a.Phase)})
	}
	labelW := visibleWidth("last msg")
	if w := visibleWidth("totals"); w > labelW {
		labelW = w
	}
	for _, r := range rows {
		if w := visibleWidth(r[0]); w > labelW {
			labelW = w
		}
	}
	msg := s.FixAgent.LastMessage
	if msg == "" {
		msg = "n/a"
	}
	vw := width - labelW - 4
	if vw < 1 {
		vw = 1
	}
	if rs := []rune(msg); len(rs) <= vw {
		rows = append(rows, [2]string{"last msg", msg}, [2]string{"", ""})
	} else {
		rows = append(rows, [2]string{"last msg", string(rs[:vw])}, [2]string{"", string(rs[vw:])})
	}
	t := s.FixAgent.Totals
	rows = append(rows, [2]string{"totals", fmt.Sprintf("%d att · %d fixed · %d ledg · %d fail · %d timeout · %.1f wall-h", t["total"], t["fixed"], t["ledgered"], t["failed"], t["timeout"], float64(s.FixAgent.Spend.AttemptSeconds)/3600)})
	lines = append(lines, labelGrid(rows, 1, width, color)...)
	return lines
}

func renderEvents(s StatusSnapshot, color bool, width int) []string {
	lines := []string{paneRule("EVENTS", "", width, color)}
	if len(s.Events) == 0 {
		return append(lines, "  n/a")
	}
	for _, e := range s.Events {
		lines = append(lines, "  "+paint(color, sgrDim, e.Time)+"  "+e.Message)
	}
	return lines
}

func paneRule(title, right string, width int, color bool) string {
	left := " " + paint(color, sgrCyan, title) + " "
	if stripANSI(right) == "" {
		dashes := width - visibleWidth(left)
		if dashes < 3 {
			dashes = 3
		}
		return left + paint(color, sgrDim, strings.Repeat("─", dashes))
	}
	dashes := width - visibleWidth(left) - visibleWidth(right) - 4
	if dashes < 3 {
		dashes = 3
	}
	return left + paint(color, sgrDim, strings.Repeat("─", dashes)) + " " + right + " " + paint(color, sgrDim, "──")
}

func labelGrid(rows [][2]string, cols int, width int, color bool) []string {
	if cols < 1 {
		cols = 1
	}
	labelW := make([]int, cols)
	valueW := make([]int, cols)
	for i, r := range rows {
		c := i % cols
		if w := visibleWidth(r[0]); w > labelW[c] {
			labelW[c] = w
		}
		if w := visibleWidth(r[1]); w > valueW[c] {
			valueW[c] = w
		}
	}
	var out []string
	for start := 0; start < len(rows); start += cols {
		var parts []string
		for c := 0; c < cols && start+c < len(rows); c++ {
			r := rows[start+c]
			cell := padRight(paint(color, sgrDim, r[0]), labelW[c]) + "  " + paint(color, sgrBold, r[1])
			if c < cols-1 && start+c+1 < len(rows) {
				cell = padRight(cell, labelW[c]+2+valueW[c])
			}
			parts = append(parts, cell)
		}
		out = append(out, truncateVisible("  "+strings.Join(parts, "    "), width))
	}
	return out
}

func metricTable(header []string, rows [][]string, width int, color bool) []string {
	if len(header) == 0 {
		return nil
	}
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = visibleWidth(h)
	}
	for _, row := range rows {
		for i, c := range row {
			if i < len(widths) && visibleWidth(c) > widths[i] {
				widths[i] = visibleWidth(c)
			}
		}
	}
	// The first column absorbs any overflow so the numeric columns stay intact.
	total := 2 + widths[0]
	for i := 1; i < len(widths); i++ {
		total += 2 + widths[i]
	}
	if total > width {
		if w := widths[0] - (total - width); w >= 12 {
			widths[0] = w
		} else {
			widths[0] = 12
		}
	}
	var out []string
	var hparts []string
	for i, h := range header {
		if i == 0 {
			hparts = append(hparts, padRight(h, widths[i]))
		} else {
			hparts = append(hparts, padLeft(h, widths[i]))
		}
	}
	out = append(out, "  "+paint(color, sgrDim, strings.Join(hparts, "  ")))
	for _, row := range rows {
		parts := make([]string, len(header))
		for i := range header {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			if i == 0 {
				parts[i] = padRight(truncateVisible(cell, widths[i]), widths[i])
			} else {
				parts[i] = padLeft(cell, widths[i])
			}
		}
		out = append(out, truncateVisible("  "+strings.Join(parts, "  "), width))
	}
	return out
}

func sideBySide(left, right []string, colWidth, gutter int) []string {
	n := len(left)
	if len(right) > n {
		n = len(right)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, r := "", ""
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		out = append(out, padRight(truncateVisible(l, colWidth), colWidth)+strings.Repeat(" ", gutter)+r)
	}
	return out
}

func paint(on bool, code, s string) string {
	if !on || s == "" {
		return s
	}
	return code + s + sgrReset
}

func stripANSI(s string) string {
	return ansiRe.ReplaceAllString(s, "")
}

func visibleWidth(s string) int {
	return utf8.RuneCountInString(stripANSI(s))
}

func padRight(s string, width int) string {
	if n := visibleWidth(s); n < width {
		return s + strings.Repeat(" ", width-n)
	}
	return s
}

func padLeft(s string, width int) string {
	if n := visibleWidth(s); n < width {
		return strings.Repeat(" ", width-n) + s
	}
	return s
}

func truncateVisible(s string, width int) string {
	if width <= 0 || visibleWidth(s) <= width {
		return s
	}
	plain := stripANSI(s)
	rs := []rune(plain)
	if len(rs) <= width {
		return plain
	}
	if width <= 1 {
		return string(rs[:width])
	}
	return string(rs[:width-1]) + "…"
}

func fmtCount(v any) string {
	var f float64
	switch x := v.(type) {
	case int:
		f = float64(x)
	case int64:
		f = float64(x)
	case float64:
		f = x
	default:
		return "n/a"
	}
	abs := f
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000:
		return fmt.Sprintf("%.1fM", f/1_000_000)
	case abs >= 1_000:
		return fmt.Sprintf("%.1fk", f/1_000)
	default:
		return fmt.Sprintf("%.0f", f)
	}
}

func fmtAgo(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return fmtDur(d)
}

func fmtDur(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	if d >= time.Hour {
		h := int(d / time.Hour)
		m := int((d % time.Hour) / time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	if d >= time.Minute {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	return fmt.Sprintf("%ds", int(d/time.Second))
}

func rateString(r RateValue, perMinute bool) string {
	if !r.OK {
		return "n/a"
	}
	v := r.Value
	if perMinute {
		v *= 60
	}
	if v >= 10 {
		return fmtCount(v)
	}
	return fmt.Sprintf("%.1f", v)
}

func fmtGrouped(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func signed(n int64) string {
	if n >= 0 {
		return "+" + fmtCount(n)
	}
	return "-" + fmtCount(-n)
}

func signedOrNA(m map[string]int64, key string) string {
	if m == nil {
		return "n/a"
	}
	return signed(m[key])
}

func valOrNA(n int) string {
	if n == 0 {
		return "n/a"
	}
	return strconv.Itoa(n)
}

func sinceOrNA(t, now time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return fmtDur(now.Sub(t))
}

func timeOrNA(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.Local().Format("2006-01-02 15:04")
}

func deadlineText(t, now time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	prefix := "in "
	d := t.Sub(now)
	if d < 0 {
		prefix = "over "
		d = -d
	}
	return prefix + fmtDur(d) + " (" + t.Local().Format("Jan 2 15:04") + ")"
}
