package clickhouse

import (
	"maps"
	"slices"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// When adding a new clickhouse setting to this list, check when the setting is introduced to ClickHouse
// and if applicable, add a corresponding minimum supported version below to ensure queries on older versions
// of ClickHouse servers are not impacted.
// Important: if the setting causes breaking changes to existing PeerDB flows (not just ClickHouse compatibility),
// it must also be gated by PeerDB's internal version.
const (
	SettingAllowNullableKey                   CHSetting = "allow_nullable_key"
	SettingJsonTypeEscapeDotsInKeys           CHSetting = "json_type_escape_dots_in_keys"
	SettingTypeJsonSkipDuplicatedPaths        CHSetting = "type_json_skip_duplicated_paths"
	SettingThrowOnMaxPartitionsPerInsertBlock CHSetting = "throw_on_max_partitions_per_insert_block"
	SettingParallelDistributedInsertSelect    CHSetting = "parallel_distributed_insert_select"
	SettingMaxTableSizeToDrop                 CHSetting = "max_table_size_to_drop"
)

// CHSettingMinVersions maps setting names to their minimum required ClickHouse versions that PeerDB supports.
// If minimum version is not specified, we assume the setting is available to all ClickHouse versions
var CHSettingMinVersions = map[CHSetting]chproto.Version{
	SettingJsonTypeEscapeDotsInKeys:    {Major: 25, Minor: 8, Patch: 0},
	SettingTypeJsonSkipDuplicatedPaths: {Major: 24, Minor: 8, Patch: 0},
	SettingMaxTableSizeToDrop:          {Major: 23, Minor: 12, Patch: 0},
}

type CHSetting string

type CHSettings struct {
	settings  map[CHSetting]string
	chVersion *chproto.Version
}

type CHSettingEntry struct {
	key CHSetting
	val string
}

func GetMinVersion(name CHSetting) (chproto.Version, bool) {
	if minVersion, exists := CHSettingMinVersions[name]; exists {
		return minVersion, true
	}
	return chproto.Version{}, false
}

// NewCHSettingsString is a one-liner method to generate an immutable settings string
func NewCHSettingsString(version *chproto.Version, key CHSetting, val string) string {
	sg := NewCHSettings(version)
	sg.Add(key, val)
	return sg.String()
}

func NewCHSettings(version *chproto.Version, settings ...CHSettingEntry) *CHSettings {
	newSettings := make(map[CHSetting]string)
	for _, s := range settings {
		newSettings[s.key] = s.val
	}
	chSettings := &CHSettings{
		settings:  newSettings,
		chVersion: version,
	}
	return chSettings
}

func (sg *CHSettings) Add(key CHSetting, val string) *CHSettings {
	sg.settings[key] = val
	return sg
}

// String generates settings string ' SETTINGS <key1> = <val1>, <key2> = <val2>, ...';
// If ClickHouse version is set in the CHSettings, settings that do not meet CH version
// requirement will be filtered out. Otherwise, all settings are included.
func (sg *CHSettings) String() string {
	if len(sg.settings) == 0 {
		return ""
	}

	// sort keys alphabetically for consistent output
	names := slices.Collect(maps.Keys(sg.settings))
	slices.SortFunc(names, func(a, b CHSetting) int {
		return strings.Compare(string(a), string(b))
	})

	first := true
	var sb strings.Builder
	for _, name := range names {
		if minVersion, exists := GetMinVersion(name); exists && sg.chVersion != nil {
			if !chproto.CheckMinVersion(minVersion, *sg.chVersion) {
				continue
			}
		}

		if first {
			sb.WriteString(" SETTINGS ")
			first = false
		} else {
			sb.WriteString(", ")
		}
		sb.WriteString(string(name))
		sb.WriteString("=")
		sb.WriteString(sg.settings[name])
	}
	return sb.String()
}
