package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TableNameMapping(tableMappings []*protos.TableMapping, resync bool) map[string]string {
	tblNameMapping := make(map[string]string, len(tableMappings))
	if resync {
		for _, mapping := range tableMappings {
			if mapping.Engine != protos.TableEngine_CH_ENGINE_NULL {
				mapping.DestinationTableIdentifier += "_resync"
			}
		}
	}
	for _, v := range tableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}

	return tblNameMapping
}

func FetchConfigFromDB(ctx context.Context, catalogPool shared.CatalogPool, flowName string) (*protos.FlowConnectionConfigsCore, error) {
	var configBytes sql.RawBytes
	if err := catalogPool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1 LIMIT 1", flowName,
	).Scan(&configBytes); err != nil {
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	var cfgFromDB protos.FlowConnectionConfigsCore
	if err := proto.Unmarshal(configBytes, &cfgFromDB); err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &cfgFromDB, nil
}

func TableMappingsToBytes(tableMappings []*protos.TableMapping) ([][]byte, error) {
	tableMappingsBytes := [][]byte{}
	for _, tableMapping := range tableMappings {
		tableMappingBytes, err := proto.Marshal(tableMapping)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal table mapping to migrate to catalog: %w", err)
		}
		tableMappingsBytes = append(tableMappingsBytes, tableMappingBytes)
	}
	return tableMappingsBytes, nil
}

func AddTableToTableMappings(
	ctx context.Context, flowJobName string, tableMapping []*protos.TableMapping, version uint32,
) (*uint32, error) {
	logger := LoggerFromCtx(ctx)
	pool, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction to add table to table mappings: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	existingTableMappings, err := FetchTableMappingsFromDB(ctx, flowJobName, version)
	if err != nil {
		return nil, fmt.Errorf("unable to load existing table mappings: %w", err)
	}

	slog.Info("Y0$$$")

	existingTableMappings = append(existingTableMappings, tableMapping...)
	slog.Info("Updated table mappings", slog.Int("num_mappings", len(existingTableMappings)), slog.Any("mappings", existingTableMappings))
	slog.Info("Y0$$$$$$$")

	tableMappingsBytes, err := TableMappingsToBytes(existingTableMappings)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal table mappings: %w", err)
	}
	version += 1

	jsonBlob, _ := json.MarshalIndent(existingTableMappings, "", "  ")
	stmt := `INSERT INTO table_mappings (flow_name, version, table_mappings, json_blob) VALUES ($1, $2, $3, $4)
	 ON CONFLICT (flow_name, version) DO UPDATE SET table_mappings = EXCLUDED.table_mappings`
	_, err = tx.Exec(ctx, stmt, flowJobName, version, tableMappingsBytes, jsonBlob)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement to add table to table mappings: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction to add table to table mappings: %w", err)
	}

	return &version, nil
}

func FetchTableMappingsFromDB(ctx context.Context, flowJobName string, version uint32) ([]*protos.TableMapping, error) {
	pool, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	row := pool.QueryRow(ctx,
		`select table_mappings
	 from table_mappings
	 where flow_name = $1
	   and version   = $2`,
		flowJobName, version,
	)

	slog.Info("!!!!! Fetching TABLE MAPPINGS", slog.String("flow_name", flowJobName), slog.Int("version", int(version)))
	var tableMappingsBytes [][]byte
	if err := row.Scan(&tableMappingsBytes); err != nil {
		return nil, fmt.Errorf("failed to deserialize table mapping schema proto: %w", err)
	}

	//var tableMappings []*protos.TableMapping

	var tableMappings []*protos.TableMapping = []*protos.TableMapping{}
	slog.Info("!!!!! YO3 ", slog.Int("len", len(tableMappingsBytes)))

	for _, tableMappingBytes := range tableMappingsBytes {
		var tableMapping protos.TableMapping
		slog.Info("!!!!! YO - in loop ")
		if err := proto.Unmarshal(tableMappingBytes, &tableMapping); err != nil {
			return nil, err
		}

		tableMappings = append(tableMappings, &tableMapping)
	}
	return tableMappings, nil
}
