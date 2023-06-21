package activities

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// FetchConfigActivityInput is the input for the FetchConfigActivity.
type FetchConfigActivityInput struct {
	// The JDBC URL for the catalog database.
	CatalogJdbcURL string
	// The name of the peer flow to fetch the config for.
	PeerFlowName string
}

// FetchConfigActivity is an activity that fetches the config for the specified peer flow.
// This activity is invoked by the PeerFlowWorkflow.
type FetchConfigActivity struct{}

// FetchConfig retrieves the source and destination config.
func (a *FetchConfigActivity) FetchConfig(
	ctx context.Context,
	input *FetchConfigActivityInput,
) (*protos.FlowConnectionConfigs, error) {
	pool, err := pgxpool.New(ctx, input.CatalogJdbcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	sourceConnectionConfig, err := FetchPeerConfig(ctx, pool, input.PeerFlowName, "source_peer")
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal source connection config: %w", err)
	}

	destinationConnectionConfig, err := FetchPeerConfig(ctx, pool, input.PeerFlowName, "destination_peer")
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal destination connection config: %w", err)
	}

	query := `SELECT source_table_identifier, destination_table_identifier FROM flows WHERE name = $1`
	rows, err := pool.Query(ctx, query, input.PeerFlowName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table identifiers: %w", err)
	}
	defer rows.Close()
	// Create a map to store the mapping of source table to destination table
	tableNameMapping := make(map[string]string)
	var srcTableIdentifier, dstTableIdentifier string

	// Iterate over all the result rows
	for rows.Next() {
		err = rows.Scan(&srcTableIdentifier, &dstTableIdentifier)
		if err != nil {
			return nil, fmt.Errorf("error scanning row %w", err)
		}

		// Store the tableNameMapping in the map
		tableNameMapping[srcTableIdentifier] = dstTableIdentifier
	}

	log.Printf("successfully fetched config for peer flow - %s", input.PeerFlowName)

	return &protos.FlowConnectionConfigs{
		Source:           sourceConnectionConfig,
		Destination:      destinationConnectionConfig,
		TableNameMapping: tableNameMapping,
	}, nil
}

// fetchPeerConfig retrieves the config for a given peer by join label.
func FetchPeerConfig(ctx context.Context, pool *pgxpool.Pool, flowName string, label string) (*protos.Peer, error) {
	var name string
	var dbtype int32
	var opts []byte

	query := fmt.Sprintf(
		"SELECT e.name, e.type, e.options FROM flows f JOIN peers e ON f.%s = e.id WHERE f.name = $1",
		label)
	err := pool.QueryRow(ctx, query, flowName).Scan(&name, &dbtype, &opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch config for %s: %w", label, err)
	}

	res := &protos.Peer{
		Name: name,
		Type: protos.DBType(dbtype),
	}

	switch protos.DBType(dbtype) {
	case protos.DBType_BIGQUERY:
		var peerConfig protos.BigqueryConfig
		err = proto.Unmarshal(opts, &peerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal bigquery config: %w", err)
		}
		res.Config = &protos.Peer_BigqueryConfig{BigqueryConfig: &peerConfig}
	case protos.DBType_POSTGRES:
		var peerConfig protos.PostgresConfig
		err = proto.Unmarshal(opts, &peerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal postgres config: %w", err)
		}
		res.Config = &protos.Peer_PostgresConfig{PostgresConfig: &peerConfig}
	case protos.DBType_SNOWFLAKE:
		var peerConfig protos.SnowflakeConfig
		err = proto.Unmarshal(opts, &peerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal snowflake config: %w", err)
		}
		res.Config = &protos.Peer_SnowflakeConfig{SnowflakeConfig: &peerConfig}
	default:
		return nil, fmt.Errorf("unsupported database type: %d", dbtype)
	}

	return res, nil
}
