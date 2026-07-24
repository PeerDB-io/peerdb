package conncockroachdb

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	geom "github.com/twpayne/go-geos"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// This file contains the transport-agnostic parts of CockroachDB changefeed
// consumption: HLC timestamp handling, changefeed statement construction and
// wrapped-envelope JSON decoding into QValues. The envelope format is shared
// by sinkless, webhook and Kafka changefeed sinks, so cdc.go only layers the
// "run the changefeed and yield rows" transport on top of these functions.

const (
	minChangefeedResolvedInterval = time.Second
	maxChangefeedResolvedInterval = 10 * time.Second

	defaultChangefeedMaxRetries     = uint32(5)
	defaultChangefeedRetryBaseDelay = 500 * time.Millisecond
	maxChangefeedRetryDelay         = 30 * time.Second

	// CockroachDB couples throughput and checkpointing across all tables
	// watched by one changefeed; past this count warn about the coupling.
	changefeedManyTablesThreshold = 100
)

// parseHLCWallNanos extracts the wall-clock nanosecond component of a
// CockroachDB HLC timestamp ("<wallnanos>.<logical>"), which doubles as
// PeerDB's int64 CDC offset.
func parseHLCWallNanos(hlc string) (int64, error) {
	if !crdbSystemTimeRegex.MatchString(hlc) {
		return 0, fmt.Errorf("invalid CockroachDB HLC timestamp %q", hlc)
	}
	wall, _, _ := strings.Cut(hlc, ".")
	nanos, err := strconv.ParseInt(wall, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid CockroachDB HLC timestamp %q: %w", hlc, err)
	}
	return nanos, nil
}

// changefeedResolvedInterval picks the resolved-timestamp cadence for a batch:
// frequent enough to checkpoint within the sync interval without spamming the cluster.
func changefeedResolvedInterval(idleTimeout time.Duration) time.Duration {
	return min(max(idleTimeout/4, minChangefeedResolvedInterval), maxChangefeedResolvedInterval)
}

type changefeedOptions struct {
	Cursor           string
	ResolvedInterval time.Duration
}

func buildChangefeedStatement(tables []*common.QualifiedTable, opts changefeedOptions) (string, error) {
	if len(tables) == 0 {
		return "", errors.New("changefeed requires at least one table")
	}
	// cursor is embedded in the statement; the regex both validates the HLC
	// format and guards against injection
	if !crdbSystemTimeRegex.MatchString(opts.Cursor) {
		return "", fmt.Errorf("invalid changefeed cursor %q", opts.Cursor)
	}
	quoted := make([]string, len(tables))
	for i, table := range tables {
		quoted[i] = table.String()
	}
	intervalMs := max(opts.ResolvedInterval, minChangefeedResolvedInterval).Milliseconds()
	// schema_change_policy is left at its default 'backfill': ADD COLUMN with
	// a default re-emits the whole table as updates, and resolved timestamps
	// can stall during large backfills, which the watchdog window must ride out.
	return fmt.Sprintf("CREATE CHANGEFEED FOR TABLE %s WITH envelope = 'wrapped', updated, diff,"+
		" resolved = '%dms', min_checkpoint_frequency = '%dms', full_table_name, initial_scan = 'no', cursor = '%s'",
		strings.Join(quoted, ", "), intervalMs, intervalMs, opts.Cursor), nil
}

// changefeedEnvelope is the wrapped-envelope JSON emitted per changefeed message.
// Data messages carry after/before/updated; resolved messages only carry resolved.
type changefeedEnvelope struct {
	After    map[string]json.RawMessage `json:"after"`
	Before   map[string]json.RawMessage `json:"before"`
	Updated  string                     `json:"updated"`
	Resolved string                     `json:"resolved"`
}

func parseChangefeedEnvelope(value []byte) (*changefeedEnvelope, error) {
	var envelope changefeedEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return nil, fmt.Errorf("failed to parse changefeed envelope: %w", err)
	}
	return &envelope, nil
}

type changefeedOperation int8

const (
	changefeedOpSkip changefeedOperation = iota
	changefeedOpInsert
	changefeedOpUpdate
	changefeedOpDelete
)

// operation classifies a data message: deletes arrive as after=null, and the
// diff option makes before the previous row image (null for inserts).
func (e *changefeedEnvelope) operation() changefeedOperation {
	switch {
	case e.After != nil && e.Before == nil:
		return changefeedOpInsert
	case e.After != nil:
		return changefeedOpUpdate
	case e.Before != nil:
		return changefeedOpDelete
	default:
		return changefeedOpSkip
	}
}

// changefeedTableSchema caches per-table column typing for envelope decoding.
type changefeedTableSchema struct {
	fields          map[string]types.QField
	ignored         map[string]struct{}
	primaryKeys     []string
	nullableEnabled bool
}

func newChangefeedTableSchema(schema *protos.TableSchema) *changefeedTableSchema {
	fields := make(map[string]types.QField, len(schema.Columns))
	for _, col := range schema.Columns {
		fields[col.Name] = types.QField{
			Name:     col.Name,
			Type:     types.QValueKind(col.Type),
			Nullable: col.Nullable,
		}
	}
	return &changefeedTableSchema{
		fields:          fields,
		ignored:         make(map[string]struct{}),
		primaryKeys:     schema.PrimaryKeyColumns,
		nullableEnabled: schema.NullableEnabled,
	}
}

// unknownColumns returns row keys absent from the cached schema, excluded
// columns and previously ignored columns aside. Non-empty output means the
// source table gained columns since the schema was cached: changefeeds do not
// emit DDL, so added columns are detected from the row payload itself.
func (s *changefeedTableSchema) unknownColumns(rows ...map[string]json.RawMessage) []string {
	var unknown []string
	for _, row := range rows {
		for col := range row {
			if _, ok := s.fields[col]; ok {
				continue
			}
			if _, ok := s.ignored[col]; ok {
				continue
			}
			if !slices.Contains(unknown, col) {
				unknown = append(unknown, col)
			}
		}
	}
	slices.Sort(unknown)
	return unknown
}

func changefeedRecordItems(
	row map[string]json.RawMessage,
	schema *changefeedTableSchema,
	exclude map[string]struct{},
) (model.RecordItems, error) {
	items := model.NewRecordItems(len(row))
	for col, raw := range row {
		if _, excluded := exclude[col]; excluded {
			continue
		}
		field, ok := schema.fields[col]
		if !ok {
			continue
		}
		qv, err := qvalueFromChangefeedJSON(field, raw)
		if err != nil {
			return items, fmt.Errorf("column %s as %s: %w"+
				" (the source column type may have changed; resync the mirror to pick up the new type)",
				col, field.Type, err)
		}
		items.AddColumn(col, qv)
	}
	return items, nil
}

// changefeedKeyItems reconstructs delete items from the changefeed key column,
// which is a JSON array of primary key values in primary-key column order.
// It backs deletes whose envelope carries neither an after nor a before image.
func changefeedKeyItems(
	key []byte,
	schema *changefeedTableSchema,
	exclude map[string]struct{},
) (model.RecordItems, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(key, &elems); err != nil {
		return model.RecordItems{}, fmt.Errorf("failed to parse changefeed key %s: %w", key, err)
	}
	if len(schema.primaryKeys) == 0 || len(elems) != len(schema.primaryKeys) {
		return model.RecordItems{}, fmt.Errorf("changefeed key %s does not match primary key columns %v",
			key, schema.primaryKeys)
	}
	items := model.NewRecordItems(len(elems))
	for i, col := range schema.primaryKeys {
		if _, excluded := exclude[col]; excluded {
			continue
		}
		field, ok := schema.fields[col]
		if !ok {
			return items, fmt.Errorf("primary key column %s missing from cached schema", col)
		}
		qv, err := qvalueFromChangefeedJSON(field, elems[i])
		if err != nil {
			return items, fmt.Errorf("primary key column %s: %w", col, err)
		}
		items.AddColumn(col, qv)
	}
	return items, nil
}

// tableRoutingKeys returns the candidate lookup forms of a dotted table name:
// as given, with identifier quoting stripped, and lowercased variants of both,
// so routing tolerates quoting and case differences between the parsed source
// identifier and the changefeed's full_table_name output.
func tableRoutingKeys(name string) []string {
	keys := make([]string, 0, 4)
	for _, stripped := range []string{name, stripTableNameQuotes(name)} {
		for _, key := range []string{stripped, strings.ToLower(stripped)} {
			if !slices.Contains(keys, key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// stripTableNameQuotes removes double-quote identifier quoting from a dotted
// table name (db.schema."Table" -> db.schema.Table), unescaping doubled quotes.
func stripTableNameQuotes(name string) string {
	if !strings.ContainsRune(name, '"') {
		return name
	}
	var sb strings.Builder
	sb.Grow(len(name))
	inQuotes := false
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			if inQuotes && i+1 < len(name) && name[i+1] == '"' {
				sb.WriteByte('"')
				i++
			} else {
				inQuotes = !inQuotes
			}
			continue
		}
		sb.WriteByte(name[i])
	}
	return sb.String()
}

func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

func jsonString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("expected JSON string, got %s: %w", raw, err)
	}
	return s, nil
}

// qvalueFromChangefeedJSON converts one changefeed JSON column value into the
// QValue matching field.Type.
func qvalueFromChangefeedJSON(field types.QField, raw json.RawMessage) (types.QValue, error) {
	if isJSONNull(raw) {
		return types.QValueNull(field.Type), nil
	}

	switch field.Type {
	case types.QValueKindBoolean:
		var v bool
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		return types.QValueBoolean{Val: v}, nil
	case types.QValueKindInt16:
		v, err := strconv.ParseInt(string(raw), 10, 16)
		if err != nil {
			return nil, err
		}
		return types.QValueInt16{Val: int16(v)}, nil
	case types.QValueKindInt32:
		v, err := strconv.ParseInt(string(raw), 10, 32)
		if err != nil {
			return nil, err
		}
		return types.QValueInt32{Val: int32(v)}, nil
	case types.QValueKindInt64:
		v, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return nil, err
		}
		return types.QValueInt64{Val: v}, nil
	case types.QValueKindFloat32:
		v, err := parseChangefeedFloat(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueFloat32{Val: float32(v)}, nil
	case types.QValueKindFloat64:
		v, err := parseChangefeedFloat(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueFloat64{Val: v}, nil
	case types.QValueKindNumeric:
		num, valid := parseChangefeedNumeric(raw)
		if !valid {
			// NaN/Infinity, like the QRep path
			if field.Nullable {
				return types.QValueNull(types.QValueKindNumeric), nil
			}
			return types.QValueNumeric{Precision: field.Precision, Scale: field.Scale}, nil
		}
		return types.QValueNumeric{Val: num, Precision: field.Precision, Scale: field.Scale}, nil
	case types.QValueKindString:
		s, err := jsonString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueString{Val: s}, nil
	case types.QValueKindEnum:
		s, err := jsonString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueEnum{Val: s}, nil
	case types.QValueKindBytes:
		s, err := jsonString(raw)
		if err != nil {
			return nil, err
		}
		b, err := decodeChangefeedBytes(s)
		if err != nil {
			return nil, err
		}
		return types.QValueBytes{Val: b}, nil
	case types.QValueKindUUID:
		s, err := jsonString(raw)
		if err != nil {
			return nil, err
		}
		u, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %w", err)
		}
		return types.QValueUUID{Val: u}, nil
	case types.QValueKindJSON:
		return types.QValueJSON{Val: string(raw)}, nil
	case types.QValueKindArrayJSON:
		return types.QValueJSON{Val: string(raw), IsArray: true}, nil
	case types.QValueKindTimestamp:
		v, err := parseWithString(raw, parseChangefeedTimestamp)
		if err != nil {
			return nil, err
		}
		return types.QValueTimestamp{Val: v}, nil
	case types.QValueKindTimestampTZ:
		v, err := parseWithString(raw, parseChangefeedTimestampTZ)
		if err != nil {
			return nil, err
		}
		return types.QValueTimestampTZ{Val: v}, nil
	case types.QValueKindDate:
		v, err := parseWithString(raw, parseChangefeedDate)
		if err != nil {
			return nil, err
		}
		return types.QValueDate{Val: v}, nil
	case types.QValueKindTime:
		v, err := parseWithString(raw, parseChangefeedTime)
		if err != nil {
			return nil, err
		}
		return types.QValueTime{Val: v}, nil
	case types.QValueKindTimeTZ:
		v, err := parseWithString(raw, parseTimeTZ)
		if err != nil {
			return nil, err
		}
		return types.QValueTimeTZ{Val: v}, nil
	case types.QValueKindInterval:
		v, err := parseWithString(raw, parsePgIntervalToJSON)
		if err != nil {
			return nil, err
		}
		return types.QValueInterval{Val: v}, nil
	case types.QValueKindINET:
		s, err := jsonString(raw)
		if err != nil {
			return nil, err
		}
		return types.QValueINET{Val: s}, nil
	case types.QValueKindGeography, types.QValueKindGeometry:
		wkt, err := geoJSONToWKT(raw)
		if err != nil {
			// mirror the QRep fallback for unparseable geo values
			if field.Nullable {
				return types.QValueNull(field.Type), nil
			} else if field.Type == types.QValueKindGeography {
				return types.QValueGeography{}, nil
			}
			return types.QValueGeometry{}, nil
		}
		if field.Type == types.QValueKindGeography {
			return types.QValueGeography{Val: wkt}, nil
		}
		return types.QValueGeometry{Val: wkt}, nil
	case types.QValueKindArrayBoolean:
		a, err := changefeedArray(raw, func(el json.RawMessage) (bool, error) {
			var v bool
			err := json.Unmarshal(el, &v)
			return v, err
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayBoolean{Val: a}, nil
	case types.QValueKindArrayInt16:
		a, err := changefeedArray(raw, func(el json.RawMessage) (int16, error) {
			v, err := strconv.ParseInt(string(el), 10, 16)
			return int16(v), err
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt16{Val: a}, nil
	case types.QValueKindArrayInt32:
		a, err := changefeedArray(raw, func(el json.RawMessage) (int32, error) {
			v, err := strconv.ParseInt(string(el), 10, 32)
			return int32(v), err
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt32{Val: a}, nil
	case types.QValueKindArrayInt64:
		a, err := changefeedArray(raw, func(el json.RawMessage) (int64, error) {
			return strconv.ParseInt(string(el), 10, 64)
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInt64{Val: a}, nil
	case types.QValueKindArrayFloat32:
		a, err := changefeedArray(raw, func(el json.RawMessage) (float32, error) {
			v, err := parseChangefeedFloat(el)
			return float32(v), err
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat32{Val: a}, nil
	case types.QValueKindArrayFloat64:
		a, err := changefeedArray(raw, parseChangefeedFloat)
		if err != nil {
			return nil, err
		}
		return types.QValueArrayFloat64{Val: a}, nil
	case types.QValueKindArrayNumeric:
		a, err := changefeedArray(raw, func(el json.RawMessage) (decimal.Decimal, error) {
			num, _ := parseChangefeedNumeric(el)
			return num, nil
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayNumeric{Val: a, Precision: field.Precision, Scale: field.Scale}, nil
	case types.QValueKindArrayUUID:
		a, err := changefeedArray(raw, func(el json.RawMessage) (uuid.UUID, error) {
			if isJSONNull(el) {
				return uuid.Nil, nil
			}
			s, err := jsonString(el)
			if err != nil {
				return uuid.Nil, err
			}
			return uuid.Parse(s)
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayUUID{Val: a}, nil
	case types.QValueKindArrayDate, types.QValueKindArrayTimestamp, types.QValueKindArrayTimestampTZ:
		parseElem := parseChangefeedDate
		switch field.Type {
		case types.QValueKindArrayTimestamp:
			parseElem = parseChangefeedTimestamp
		case types.QValueKindArrayTimestampTZ:
			parseElem = parseChangefeedTimestampTZ
		}
		a, err := changefeedArray(raw, func(el json.RawMessage) (time.Time, error) {
			if isJSONNull(el) {
				return time.Time{}, nil
			}
			return parseWithString(el, parseElem)
		})
		if err != nil {
			return nil, err
		}
		switch field.Type {
		case types.QValueKindArrayDate:
			return types.QValueArrayDate{Val: a}, nil
		case types.QValueKindArrayTimestamp:
			return types.QValueArrayTimestamp{Val: a}, nil
		default:
			return types.QValueArrayTimestampTZ{Val: a}, nil
		}
	case types.QValueKindArrayInterval:
		a, err := changefeedArray(raw, func(el json.RawMessage) (string, error) {
			if isJSONNull(el) {
				return "", nil
			}
			return parseWithString(el, parsePgIntervalToJSON)
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayInterval{Val: a}, nil
	case types.QValueKindArrayString:
		a, err := changefeedArray(raw, func(el json.RawMessage) (string, error) {
			if isJSONNull(el) {
				return "", nil
			}
			return jsonString(el)
		})
		if err != nil {
			return nil, err
		}
		return types.QValueArrayString{Val: a}, nil
	default:
		if s, err := jsonString(raw); err == nil {
			return types.QValueString{Val: s}, nil
		}
		return types.QValueString{Val: string(raw)}, nil
	}
}

func parseWithString[T any](raw json.RawMessage, parse func(string) (T, error)) (T, error) {
	s, err := jsonString(raw)
	if err != nil {
		var none T
		return none, err
	}
	return parse(s)
}

func changefeedArray[T any](raw json.RawMessage, conv func(json.RawMessage) (T, error)) ([]T, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, fmt.Errorf("expected JSON array, got %s: %w", raw, err)
	}
	out := make([]T, 0, len(elems))
	for _, el := range elems {
		v, err := conv(el)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func parseChangefeedFloat(raw json.RawMessage) (float64, error) {
	s := string(raw)
	if s != "" && s[0] == '"' {
		// NaN and +/-Infinity are emitted as JSON strings
		var err error
		if s, err = jsonString(raw); err != nil {
			return 0, err
		}
	}
	return strconv.ParseFloat(s, 64)
}

func parseChangefeedNumeric(raw json.RawMessage) (decimal.Decimal, bool) {
	s := string(raw)
	if s != "" && s[0] == '"' {
		var err error
		if s, err = jsonString(raw); err != nil {
			return decimal.Decimal{}, false
		}
	}
	num, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, false
	}
	return num, true
}

// decodeChangefeedBytes decodes the Postgres hex escape form ("\x0102")
// CockroachDB uses for BYTES values in changefeed JSON.
func decodeChangefeedBytes(s string) ([]byte, error) {
	if hexPart, ok := strings.CutPrefix(s, `\x`); ok {
		return hex.DecodeString(hexPart)
	}
	return []byte(s), nil
}

var changefeedTimestampLayouts = [...]string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
}

func parseChangefeedTimestamp(s string) (time.Time, error) {
	for _, layout := range changefeedTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamp %q", s)
}

// changefeedTimestampTZLayouts mirrors timeTZLayouts: CockroachDB emits
// hour-only offsets like 2024-01-01T14:36:34.873+00, and after an ALTER
// between TIMESTAMP and TIMESTAMPTZ zoneless values can reach the zoned path,
// where they parse as UTC.
var changefeedTimestampTZLayouts = [...]string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999-0700",
	"2006-01-02T15:04:05.999999999-07",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-0700",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
}

func parseChangefeedTimestampTZ(s string) (time.Time, error) {
	for _, layout := range changefeedTimestampTZLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamptz %q", s)
}

func parseChangefeedDate(s string) (time.Time, error) {
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date %q: %w", s, err)
	}
	return t, nil
}

func parseChangefeedTime(s string) (time.Duration, error) {
	// CockroachDB permits this extreme value for time
	s = strings.Replace(s, "24:00:00", "23:59:59.999999", 1)
	t, err := time.Parse("15:04:05.999999999", s)
	if err != nil {
		return 0, fmt.Errorf("failed to parse time %q: %w", s, err)
	}
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond()), nil
}

// parsePgIntervalToJSON converts the Postgres-style interval text emitted by
// changefeeds (e.g. "1 year 2 days 03:04:05.5") into the PeerDBInterval JSON
// representation used by the QRep path.
func parsePgIntervalToJSON(s string) (string, error) {
	interval := datatypes.PeerDBInterval{Valid: true}
	fields := strings.Fields(s)
	for i := 0; i < len(fields); {
		if strings.Contains(fields[i], ":") {
			if i != len(fields)-1 {
				return "", fmt.Errorf("failed to parse interval %q", s)
			}
			hms := fields[i]
			negative := strings.HasPrefix(hms, "-")
			hms = strings.TrimPrefix(strings.TrimPrefix(hms, "-"), "+")
			parts := strings.Split(hms, ":")
			if len(parts) != 3 {
				return "", fmt.Errorf("failed to parse interval %q", s)
			}
			hours, err := strconv.Atoi(parts[0])
			if err != nil {
				return "", fmt.Errorf("failed to parse interval %q: %w", s, err)
			}
			minutes, err := strconv.Atoi(parts[1])
			if err != nil {
				return "", fmt.Errorf("failed to parse interval %q: %w", s, err)
			}
			seconds, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return "", fmt.Errorf("failed to parse interval %q: %w", s, err)
			}
			if negative {
				hours, minutes, seconds = -hours, -minutes, -seconds
			}
			interval.Hours = hours
			interval.Minutes = minutes
			interval.Seconds = seconds
			break
		}

		if i+1 >= len(fields) {
			return "", fmt.Errorf("failed to parse interval %q", s)
		}
		n, err := strconv.Atoi(fields[i])
		if err != nil {
			return "", fmt.Errorf("failed to parse interval %q: %w", s, err)
		}
		switch strings.TrimSuffix(fields[i+1], "s") {
		case "year":
			interval.Years = n
		case "mon", "month":
			interval.Months = n
		case "day":
			interval.Days = n
		default:
			return "", fmt.Errorf("failed to parse interval %q: unknown unit %q", s, fields[i+1])
		}
		i += 2
	}

	intervalJSON, err := json.Marshal(interval)
	if err != nil {
		return "", fmt.Errorf("failed to marshal interval: %w", err)
	}
	return string(intervalJSON), nil
}

// geoJSONToWKT converts the GeoJSON objects changefeeds emit for
// geometry/geography columns into WKT, matching the QRep representation.
// Note GeoJSON does not carry an SRID, so any non-default SRID is lost.
func geoJSONToWKT(raw json.RawMessage) (string, error) {
	g, err := geom.NewGeomFromGeoJSON(string(raw))
	if err != nil {
		return "", err
	}
	wkt := g.ToWKT()
	if srid := g.SRID(); srid != 0 {
		wkt = fmt.Sprintf("SRID=%d;%s", srid, wkt)
	}
	return wkt, nil
}

func changefeedRetryConfig(config *protos.CockroachDBConfig) (uint32, time.Duration) {
	maxRetries := defaultChangefeedMaxRetries
	if config.MaxRetries != nil {
		maxRetries = config.GetMaxRetries()
	}
	baseDelay := defaultChangefeedRetryBaseDelay
	if config.RetryBaseDelayMs != nil && config.GetRetryBaseDelayMs() > 0 {
		baseDelay = time.Duration(config.GetRetryBaseDelayMs()) * time.Millisecond
	}
	return maxRetries, baseDelay
}

// changefeedBackoff computes the exponential reconnect delay for the given
// 1-based attempt, capped at maxChangefeedRetryDelay.
func changefeedBackoff(baseDelay time.Duration, attempt uint32) time.Duration {
	if attempt <= 1 {
		return min(baseDelay, maxChangefeedRetryDelay)
	}
	shift := attempt - 1
	if shift > 30 {
		return maxChangefeedRetryDelay
	}
	delay := baseDelay << shift
	if delay <= 0 || delay > maxChangefeedRetryDelay {
		return maxChangefeedRetryDelay
	}
	return delay
}

// gcThresholdErrorSubstring appears when CREATE CHANGEFEED is given a cursor
// older than the tables' gc.ttlseconds window: the MVCC history needed to
// resume has been garbage collected and retrying can never succeed.
const gcThresholdErrorSubstring = "must be after replica GC threshold"

func isCursorTooOldError(err error) bool {
	return err != nil && strings.Contains(err.Error(), gcThresholdErrorSubstring)
}

func isPermanentChangefeedError(err error) bool {
	if err == nil {
		return false
	}
	// GC threshold errors carry no dedicated SQLSTATE, match on message
	if isCursorTooOldError(err) {
		return true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case pgerrcode.UndefinedTable, pgerrcode.SyntaxError:
			return true
		default:
			// 40001, 40P01, class 08 and unknown codes are all retryable
			return false
		}
	}
	// non-PgError (driver/network wrapping) fallback on message text
	msg := err.Error()
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "syntax error") ||
		strings.Contains(msg, "rangefeeds require the kv.rangefeed.enabled setting")
}
