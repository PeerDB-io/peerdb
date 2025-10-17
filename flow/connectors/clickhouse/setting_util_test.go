package connclickhouse

import (
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
)

func TestSettingGenerator(t *testing.T) {
	// Test empty settings
	version := chproto.Version{Major: 25, Minor: 8, Patch: 0}
	builder := NewSettingGenerator(&version)
	result := builder.ToString()
	require.Empty(t, result)

	// Test single setting
	version = chproto.Version{Major: 25, Minor: 8, Patch: 0}
	builder = NewSettingGenerator(&version)
	builder.AddSetting(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = builder.ToString()
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0", result)

	// Test multiple settings
	version = chproto.Version{Major: 25, Minor: 8, Patch: 0}
	builder = NewSettingGenerator(&version)
	builder.AddSetting(SettingTypeJsonSkipDuplicatedPaths, "1")
	builder.AddSetting(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = builder.ToString()
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0, type_json_skip_duplicated_paths=1", result)
}

func TestSettingGeneratorVersionFiltering(t *testing.T) {
	// settings meeting minimum version should be included
	minVersion, _ := GetMinVersion(SettingJsonTypeEscapeDotsInKeys)
	require.Equal(t, chproto.Version{Major: 25, Minor: 8, Patch: 0}, minVersion)
	builder := NewSettingGenerator(&chproto.Version{Major: 25, Minor: 8, Patch: 0})
	builder.AddSetting(SettingJsonTypeEscapeDotsInKeys, "1")
	result := builder.ToString()
	require.Equal(t, " SETTINGS json_type_escape_dots_in_keys=1", result)

	// setting not meeting minimum version should be filter out
	builder = NewSettingGenerator(&chproto.Version{Major: 25, Minor: 7, Patch: 0})
	builder.AddSetting(SettingJsonTypeEscapeDotsInKeys, "1")
	result = builder.ToString()
	require.Empty(t, result)

	// settings not having minimum version should be included
	_, minVersionExist := GetMinVersion(SettingThrowOnMaxPartitionsPerInsertBlock)
	require.False(t, minVersionExist)
	builder = NewSettingGenerator(&chproto.Version{Major: 25, Minor: 8, Patch: 0})
	builder.AddSetting(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = builder.ToString()
	require.Equal(t, " SETTINGS throw_on_max_partitions_per_insert_block=0", result)

	// setting should be included when ch version is not specified
	builder = NewSettingGenerator(nil)
	builder.AddSetting(SettingJsonTypeEscapeDotsInKeys, "1")
	builder.AddSetting(SettingThrowOnMaxPartitionsPerInsertBlock, "0")
	result = builder.ToString()
	require.Equal(t, " SETTINGS json_type_escape_dots_in_keys=1, throw_on_max_partitions_per_insert_block=0", result)
}
