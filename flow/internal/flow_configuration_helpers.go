package internal

import (
	"context"
	"database/sql"
	"fmt"

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

func FetchTableMappingsFromDB(ctx context.Context, flowJobName string, version uint32) ([]*protos.TableMapping, error) {
	pool, err := GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx,
		"select table_mappings from table_mappings where flow_name = $1 AND version = $2",
		flowJobName, version,
	)
	if err != nil {
		return nil, err
	}

	var tableMappingsBytes [][]byte
	var tableMappings []*protos.TableMapping = []*protos.TableMapping{}

	err = rows.Scan([]any{&tableMappingsBytes})
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize table mapping schema proto: %w", err)
	}

	for _, tableMappingBytes := range tableMappingsBytes {

		var tableMapping *protos.TableMapping
		if err := proto.Unmarshal(tableMappingBytes, tableMapping); err != nil {
			return nil, err
		}

		tableMappings = append(tableMappings, tableMapping)
	}

	return tableMappings, nil
}
