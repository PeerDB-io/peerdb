package internal

import (
	"context"
	"database/sql"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TableNameMapping(tableMappings []*protos.TableMapping) map[string]string {
	tblNameMapping := make(map[string]string, len(tableMappings))
	for _, v := range tableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}
	return tblNameMapping
}

func FetchConfigFromDB(flowName string) (*protos.FlowConnectionConfigs, error) {
	var configBytes sql.RawBytes
	dbCtx := context.Background()
	pool, _ := GetCatalogConnectionPoolFromEnv(dbCtx)
	defer dbCtx.Done()
	if err := pool.QueryRow(dbCtx,
		"SELECT config_proto FROM flows WHERE name = $1 LIMIT 1", flowName,
	).Scan(&configBytes); err != nil {
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	var cfgFromDB protos.FlowConnectionConfigs
	if err := proto.Unmarshal(configBytes, &cfgFromDB); err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &cfgFromDB, nil
}

// CreateMinimalConfigFromFlowJobName creates a minimal FlowConnectionConfigs with just the FlowJobName set.
// This is used when calling CDCFlowWorkflow which will fetch the full config from DB internally.
func CreateMinimalConfigFromFlowJobName(flowJobName string) *protos.FlowConnectionConfigs {
	return &protos.FlowConnectionConfigs{
		FlowJobName: flowJobName,
	}
}
