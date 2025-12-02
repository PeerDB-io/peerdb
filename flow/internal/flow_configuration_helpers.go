package internal

import (
	"context"
	"database/sql"
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

	slog.Info("!!!!! stmt", slog.String("flow_name", flowJobName), slog.Int("version", int(version)))
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
