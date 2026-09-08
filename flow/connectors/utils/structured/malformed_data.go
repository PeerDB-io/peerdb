package structured

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const MalformedDataColumn = "malformed_data"

type MalformedReason int

const (
	ReasonUnexpected MalformedReason = iota
	ReasonTypeMismatch
	ReasonNaN
)

func (r MalformedReason) String() string {
	switch r {
	case ReasonUnexpected:
		return "unexpected"
	case ReasonTypeMismatch:
		return "type_mismatch"
	case ReasonNaN:
		return "not_a_number"
	default:
		return ""
	}
}

// This type represents malformed data state.
// As structured is applied to a document based on a ClickHouse schema, problematic fields
// get recorded in this state.
type MalformedData struct {
	// Map from seen problematic fields to their respective reasons.
	reasons map[string]MalformedReason
	// Optionally, the problematic values
	values map[string]types.QValue
}

// NewMalformedData creates a new instance of MalformedData.
func NewMalformedData() *MalformedData {
	return &MalformedData{
		reasons: make(map[string]MalformedReason),
		values:  make(map[string]types.QValue),
	}
}

// AddField adds a problematic field along with its reason and optionally its value to the MalformedData.
func (m *MalformedData) AddField(field string, reason MalformedReason, value *types.QValue) {
	m.reasons[field] = reason
	if value != nil {
		m.values[field] = *value
	}
}

func (m *MalformedData) IsEmpty() bool {
	return len(m.reasons) == 0
}

// MarshalJSON implements json.Marshaler, serializing the malformed fields state in the shape of a JSON object
// which is query friendly if ingested into ClickHouse:
//
//	{"malformed_data": {"<field>": {"<reason>": true, "value": <value>}}}
func (m *MalformedData) MarshalJSON() ([]byte, error) {
	fields := make(map[string]map[string]any, len(m.reasons))
	for name, reason := range m.reasons {
		reasonName := reason.String()
		if reasonName == "" {
			return nil, fmt.Errorf("malformed field %q has unknown reason %d", name, int(reason))
		}
		fields[name] = map[string]any{reasonName: true}
	}
	for name, value := range m.values {
		field, ok := fields[name]
		if !ok {
			return nil, fmt.Errorf("malformed field %q has a value but no reason", name)
		}
		// encoded from the Go value: nulls become JSON null, QValueJSON stays a (quoted) string
		var jsonValue any
		if value != nil {
			jsonValue = value.Value()
		}
		if isJSONRepresentable(jsonValue) {
			field["value"] = jsonValue
		} else {
			fields[name] = map[string]any{ReasonNaN.String(): true}
		}
	}
	return json.Marshal(map[string]any{"malformed_data": fields})
}

func (m *MalformedData) AsQValue() (types.QValue, error) {
	jsonData, err := m.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return types.QValueJSON{Val: string(jsonData)}, nil
}

// Describes the column malformed data is recorded in.
func MalformedDataFieldDescription() *protos.FieldDescription {
	return &protos.FieldDescription{
		Name:         MalformedDataColumn,
		Type:         string(types.QValueKindJSON),
		TypeModifier: -1,
		Nullable:     true,
	}
}

// isJSONRepresentable reports whether v can be encoded by encoding/json, which rejects NaN and ±Inf floats.
func isJSONRepresentable(v any) bool {
	switch f := v.(type) {
	case float64:
		return !math.IsNaN(f) && !math.IsInf(f, 0)
	case float32:
		return !math.IsNaN(float64(f)) && !math.IsInf(float64(f), 0)
	default:
		return true
	}
}
