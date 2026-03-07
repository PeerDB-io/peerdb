package clickhouse

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
)

func TestCHSettings(t *testing.T) {
	// Test empty settings
	version := chproto.Version{Major: 25, Minor: 8, Patch: 0}
	chSettings := NewCHSettings(&version)
	result := chSettings.String()
	require.Empty(t, result)

	// Test single setting
	version = chproto.Version{Major: 25, Minor: 8, Patch: 0}
	chSettings = NewCHSettings(&version)
	chSettings.Add(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = chSettings.String()
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0", result)

	// Test multiple settings
	version = chproto.Version{Major: 25, Minor: 8, Patch: 0}
	chSettings = NewCHSettings(&version)
	chSettings.Add(SettingTypeJsonSkipDuplicatedPaths, "1")
	chSettings.Add(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = chSettings.String()
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0, type_json_skip_duplicated_paths=1", result)
}

func TestCHSettingsVersionFiltering(t *testing.T) {
	// settings meeting minimum version should be included
	minVersion, _ := GetMinVersion(SettingJsonTypeEscapeDotsInKeys)
	require.Equal(t, chproto.Version{Major: 25, Minor: 8, Patch: 0}, minVersion)
	chSettings := NewCHSettings(&chproto.Version{Major: 25, Minor: 8, Patch: 0})
	chSettings.Add(SettingJsonTypeEscapeDotsInKeys, "1")
	require.Equal(t, " SETTINGS json_type_escape_dots_in_keys=1", chSettings.String())

	// setting not meeting minimum version should be filter out
	chSettings = NewCHSettings(&chproto.Version{Major: 25, Minor: 7, Patch: 0})
	chSettings.Add(SettingJsonTypeEscapeDotsInKeys, "1")
	require.Empty(t, chSettings.String())

	// settings not having minimum version should be included
	_, minVersionExist := GetMinVersion(SettingThrowOnMaxPartitionsPerInsertBlock)
	require.False(t, minVersionExist)
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0",
		NewCHSettingsString(&chproto.Version{Major: 25, Minor: 8, Patch: 0}, SettingThrowOnMaxPartitionsPerInsertBlock, "0"))

	// setting should be included when ch version is not specified
	chSettings = NewCHSettings(nil)
	chSettings.Add(SettingJsonTypeEscapeDotsInKeys, "1")
	chSettings.Add(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	require.Equal(t, " SETTINGS json_type_escape_dots_in_keys=1, throw_on_max_partitions_per_insert_block=0",
		chSettings.String())
}
