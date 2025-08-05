package connmongo

import (
	"context"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *MongoConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	tableNames := make([]string, 0)

	dbNames, err := c.getAllDatabaseNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	for _, dbName := range dbNames {
		collNames, err := c.getCollectionNames(ctx, dbName)
		if err != nil {
			return nil, err
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
	dbNames, err := c.getAllDatabaseNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	return &protos.PeerSchemasResponse{
		Schemas: dbNames,
	}, nil
}

func (c *MongoConnector) GetTablesInSchema(ctx context.Context, schema string, cdcEnabled bool) (*protos.SchemaTablesResponse, error) {
	collectionNames, err := c.getCollectionNames(ctx, schema)
	if err != nil {
		return nil, err
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

func (c *MongoConnector) getCollectionNames(ctx context.Context, databaseName string) ([]string, error) {
	collectionNames, err := c.client.Database(databaseName).ListCollectionNames(ctx, bson.M{
		"name": bson.M{
			"$not": bson.Regex{
				Pattern: "^system\\.",
			},
		},
		"type": bson.M{
			"$ne": "view",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get collections: %w", err)
	}
	slices.Sort(collectionNames)
	return collectionNames, nil
}

// Get all database names, but excluding MongoDB's default databases
func (c *MongoConnector) getAllDatabaseNames(ctx context.Context) ([]string, error) {
	dbs, err := c.client.ListDatabaseNames(ctx, bson.M{
		"name": bson.M{
			"$not": bson.Regex{
				Pattern: "^(admin|local|config)$",
			},
		},
	})
	if err != nil {
		return nil, err
	}
	filteredDbNames := make([]string, 0, len(dbs))
	filteredDbNames = append(filteredDbNames, dbs...)
	slices.Sort(filteredDbNames)
	return filteredDbNames, nil
}
