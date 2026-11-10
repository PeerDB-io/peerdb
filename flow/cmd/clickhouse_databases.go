package cmd

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// ClickHouse database and peer names built from them: lowercase identifiers only.
var clickHouseDatabaseNameRegexp = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (h *FlowRequestHandler) GetClickHouseDatabases(
	ctx context.Context,
	req *protos.ClickHouseDatabasesRequest,
) (*protos.ClickHouseDatabasesResponse, APIError) {
	peer, conn, connClose, err := connectors.LoadPeerAndGetByNameAs[connectors.ClickHouseDatabaseConnector](
		ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("peer %s is not a ClickHouse peer: %w", req.PeerName, err))
	}
	defer connClose(ctx)

	databases, err := conn.ListDatabases(ctx)
	if err != nil {
		return nil, NewFailedPreconditionApiError(err)
	}
	return &protos.ClickHouseDatabasesResponse{
		Databases:       databases,
		CurrentDatabase: peer.GetClickhouseConfig().GetDatabase(),
	}, nil
}

// UseClickHouseDatabase returns the virtual peer name (<peer>__<database>) that makes
// a mirror write to the requested database with the given peer's server and
// credentials. No peer is created; the database is created when asked to.
func (h *FlowRequestHandler) UseClickHouseDatabase(
	ctx context.Context,
	req *protos.UseClickHouseDatabaseRequest,
) (*protos.UseClickHouseDatabaseResponse, APIError) {
	if !clickHouseDatabaseNameRegexp.MatchString(req.Database) {
		return nil, NewInvalidArgumentApiError(errors.New(
			"database name must be lowercase letters, digits and underscores, and cannot start with a digit"))
	}

	peer, conn, connClose, err := connectors.LoadPeerAndGetByNameAs[connectors.ClickHouseDatabaseConnector](
		ctx, nil, h.pool, req.PeerName)
	if err != nil {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("peer %s is not a ClickHouse peer: %w", req.PeerName, err))
	}
	defer connClose(ctx)

	config := peer.GetClickhouseConfig()
	if config == nil {
		return nil, NewInvalidArgumentApiError(fmt.Errorf("peer %s has no ClickHouse config", req.PeerName))
	}
	if strings.Contains(peer.Name, connectors.VirtualPeerSeparator) {
		return nil, NewInvalidArgumentApiError(fmt.Errorf(
			"peer %s contains %q, so it cannot select another database", peer.Name, connectors.VirtualPeerSeparator))
	}
	// Already pointing at that database: the peer itself is the destination.
	if config.Database == req.Database {
		return &protos.UseClickHouseDatabaseResponse{PeerName: peer.Name}, nil
	}

	databases, err := conn.ListDatabases(ctx)
	if err != nil {
		return nil, NewFailedPreconditionApiError(err)
	}
	exists := false
	for _, database := range databases {
		if database == req.Database {
			exists = true
			break
		}
	}
	if !exists {
		if !req.CreateDatabase {
			return nil, NewNotFoundApiError(fmt.Errorf("database %s does not exist", req.Database))
		}
		if err := conn.CreateDatabaseIfNotExists(ctx, req.Database); err != nil {
			return nil, NewFailedPreconditionApiError(err)
		}
	}

	virtualName := connectors.VirtualPeerName(peer.Name, req.Database)
	if _, err := connectors.LoadPeerType(ctx, h.pool, virtualName); err != nil {
		return nil, NewInternalApiError(err)
	}
	return &protos.UseClickHouseDatabaseResponse{PeerName: virtualName}, nil
}
