package alerting

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// secretFieldsByServiceType holds the JSON keys tagged `sensitive:"true"` in
// each service's config struct
var secretFieldsByServiceType = map[ServiceType][]string{
	SLACK: sensitiveJSONFields[slackAlertConfig](),
	EMAIL: sensitiveJSONFields[EmailAlertSenderConfig](),
}

func sensitiveJSONFields[T any]() []string {
	var fields []string
	for field := range reflect.TypeFor[T]().Fields() {
		if field.Tag.Get("sensitive") == "true" {
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			fields = append(fields, name)
		}
	}
	return fields
}

// RedactSecrets replaces every sensitive field with an empty JSON string so
// secrets are never returned to API clients.
func RedactSecrets(serviceType ServiceType, serviceConfig []byte) ([]byte, error) {
	secretFields := secretFieldsByServiceType[serviceType]
	if len(secretFields) == 0 {
		return serviceConfig, nil
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(serviceConfig, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service config for redaction: %w", err)
	}
	for _, field := range secretFields {
		if _, ok := cfg[field]; ok {
			cfg[field] = emptyJSONString
		}
	}
	return json.Marshal(cfg)
}

// MergeSecrets fills empty sensitive fields in submitted from existing, so a
// client can omit an unchanged secret to keep its stored value
func MergeSecrets(serviceType ServiceType, submitted, existing []byte) ([]byte, error) {
	secretFields := secretFieldsByServiceType[serviceType]
	if len(secretFields) == 0 {
		return submitted, nil
	}
	var submittedMap, existingMap map[string]json.RawMessage
	if err := json.Unmarshal(submitted, &submittedMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal submitted service config: %w", err)
	}
	if err := json.Unmarshal(existing, &existingMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal existing service config: %w", err)
	}
	changed := false
	for _, field := range secretFields {
		if v, ok := submittedMap[field]; ok && !isEmptyJSONString(v) {
			continue
		}
		if v, ok := existingMap[field]; ok {
			submittedMap[field] = v
			changed = true
		}
	}
	if !changed {
		return submitted, nil
	}
	return json.Marshal(submittedMap)
}

var emptyJSONString = json.RawMessage(`""`)

func isEmptyJSONString(raw json.RawMessage) bool {
	var s string
	return json.Unmarshal(raw, &s) == nil && s == ""
}
