package connclickhouse

import (
	"maps"
	"slices"
	"strings"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type CHSetting string

// when adding a new clickhouse setting to this list, check if the setting is relatively new to ClickHouse,
// add a corresponding minimum version below to ensure queries on older versions are not impacted.
const (
	SettingAllowNullableKey                   CHSetting = "allow_nullable_key"
	SettingJsonTypeEscapeDotsInKeys           CHSetting = "json_type_escape_dots_in_keys"
	SettingTypeJsonSkipDuplicatedPaths        CHSetting = "type_json_skip_duplicated_paths"
	SettingThrowOnMaxPartitionsPerInsertBlock CHSetting = "throw_on_max_partitions_per_insert_block"
	SettingParallelDistributedInsertSelect    CHSetting = "parallel_distributed_insert_select"
)

// settingMinVersions maps setting names to their minimum required ClickHouse versions
// If minimum version is not specified, we assume the setting is available to all ClickHouse versions
var settingMinVersions = map[CHSetting]chproto.Version{
	SettingJsonTypeEscapeDotsInKeys:    {Major: 25, Minor: 8, Patch: 0},
	SettingTypeJsonSkipDuplicatedPaths: {Major: 24, Minor: 8, Patch: 0},
}

func GetMinVersion(name CHSetting) (chproto.Version, bool) {
	if minVersion, exists := settingMinVersions[name]; exists {
		return minVersion, true
	}
	return chproto.Version{}, false
}

type SettingGenerator struct {
	settings  map[CHSetting]string
	chVersion *chproto.Version
}

func NewSettingGenerator(version *chproto.Version) *SettingGenerator {
	return &SettingGenerator{
		settings:  make(map[CHSetting]string),
		chVersion: version,
	}
}

func (sg *SettingGenerator) AddSetting(name CHSetting, value string) *SettingGenerator {
	sg.settings[name] = value
	return sg
}

// ToString generates settings string ' SETTINGS <key1> = <val1>, <key2> = <val2>, ...';
// If ClickHouse version is set in the SettingGenerator, settings that do not meet CH versio
// requirement will be filtered out. Otherwise, all settings are included.
func (sg *SettingGenerator) ToString() string {
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
