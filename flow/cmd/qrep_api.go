package main

import (
	"errors"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
)

const (
	SyncDataFormatAvro    = "avro"
	SyncDataFormatDefault = "default"
	WriteModeAppend       = "append"
	WriteModeUpsert       = "upsert"
)

type UnsupportedOptionError struct {
	OptionName  string
	OptionValue string
}

func (e *UnsupportedOptionError) Error() string {
	return fmt.Sprintf("unsupported %s: %s", e.OptionName, e.OptionValue)
}

func createWriteMode(writeType protos.QRepWriteType, upsertKeyColumns []string) *protos.QRepWriteMode {
	return &protos.QRepWriteMode{
		WriteType:        writeType,
		UpsertKeyColumns: upsertKeyColumns,
	}
}

func genConfigForQRepFlow(
	config *protos.QRepConfig,
	flowOptions map[string]interface{},
	queryString string,
	destinationTableIdentifier string,
) error {
	config.InitialCopyOnly = false
	config.MaxParallelWorkers = uint32(flowOptions["parallelism"].(float64))
	config.DestinationTableIdentifier = destinationTableIdentifier
	config.Query = queryString
	config.WatermarkColumn = flowOptions["watermark_column"].(string)
	config.WatermarkTable = flowOptions["watermark_table_name"].(string)

	// TODO (kaushik): investigate why this is a float64 in the first place
	config.BatchSizeInt = uint32(flowOptions["batch_size_int"].(float64))
	config.BatchDurationSeconds = uint32(flowOptions["batch_duration_timestamp"].(float64))
	config.WaitBetweenBatchesSeconds = uint32(flowOptions["refresh_interval"].(float64))
	config.NumRowsPerPartition = uint32(flowOptions["num_rows_per_partition"].(float64))

	syncDataFormat, ok := flowOptions["sync_data_format"].(string)
	if !ok {
		return errors.New("sync_data_format must be a string")
	}

	switch syncDataFormat {
	case SyncDataFormatAvro:
		config.SyncMode = protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO
		if _, ok := flowOptions["staging_path"]; ok {
			config.StagingPath = flowOptions["staging_path"].(string)
		} else {
			config.StagingPath = ""
		}
	case SyncDataFormatDefault:
		config.SyncMode = protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT
	default:
		return &UnsupportedOptionError{"sync_data_format", syncDataFormat}
	}

	mode, ok := flowOptions["mode"].(string)
	if !ok {
		return errors.New("mode must be a string")
	}

	switch mode {
	case WriteModeAppend:
		config.WriteMode = createWriteMode(protos.QRepWriteType_QREP_WRITE_MODE_APPEND, nil)
	case WriteModeUpsert:
		upsertKeyColumns := make([]string, 0)
		for _, column := range flowOptions["unique_key_columns"].([]interface{}) {
			upsertKeyColumns = append(upsertKeyColumns, column.(string))
		}
		config.WriteMode = createWriteMode(protos.QRepWriteType_QREP_WRITE_MODE_UPSERT, upsertKeyColumns)
	default:
		return &UnsupportedOptionError{"mode", mode}
	}
	return nil
}
