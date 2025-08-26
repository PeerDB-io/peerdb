package connmongo

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	shared_mongo "github.com/PeerDB-io/peerdb/flow/shared/mongo"
)

func (c *MongoConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	tableNames := make([]string, 0)

	dbNames, err := shared_mongo.GetDatabaseNames(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	for _, dbName := range dbNames {
		collNames, err := shared_mongo.GetCollectionNames(ctx, c.client, dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to get collections: %w", err)
		}
		for _, collName := range collNames {
			tableNames = append(tableNames, fmt.Sprintf("%s.%s", dbName, collName))
		}
	}
	return &protos.AllTablesResponse{
		Tables: tableNames,
	}, nil
}

func (c *MongoConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	dbNames, err := shared_mongo.GetDatabaseNames(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	return &protos.PeerSchemasResponse{
		Schemas: dbNames,
	}, nil
}

func (c *MongoConnector) GetTablesInSchema(ctx context.Context, schema string, cdcEnabled bool) (*protos.SchemaTablesResponse, error) {
	collectionNames, err := shared_mongo.GetCollectionNames(ctx, c.client, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get collections: %w", err)
	}

	response := protos.SchemaTablesResponse{
		Tables: make([]*protos.TableResponse, 0, len(collectionNames)),
	}

	for _, collectionName := range collectionNames {
		tableResp := &protos.TableResponse{
			TableName: collectionName,
			CanMirror: true,
			// TODO: implement TableSize fetching
			TableSize: "",
		}
		response.Tables = append(response.Tables, tableResp)
	}

	return &response, nil
}

func (c *MongoConnector) GetColumns(ctx context.Context, version uint32, schema string, table string) (*protos.TableColumnsResponse, error) {
	return &protos.TableColumnsResponse{
		Columns: []*protos.ColumnsItem{},
	}, nil
}
