package connmongo

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (c *MongoConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	tableNames := make([]string, 0)

	dbNames, err := c.getAllDatabaseNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get databases: %w", err)
	}
	tableNames = append(tableNames, dbNames...)
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
	db := c.client.Database(schema)
	collectionNames, err := db.ListCollectionNames(ctx, bson.D{})
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

// TODO: replace placeholder values, how should we displace source columns in mongodb?
func (c *MongoConnector) GetColumns(ctx context.Context, schema string, table string) (*protos.TableColumnsResponse, error) {
	return &protos.TableColumnsResponse{
		Columns: []*protos.ColumnsItem{
			{
				Name:  "_id",
				Type:  "ObjectId",
				IsKey: true,
			},
			{
				Name:  "_full_document",
				Type:  "string",
				IsKey: false,
			},
		},
	}, nil
}

// Get all database names, but excluding MongoDB's default databases
func (c *MongoConnector) getAllDatabaseNames(ctx context.Context) ([]string, error) {
	// TODO: investigate why query fails when this logic is added to the filter
	excludedDatabaseSet := map[string]bool{
		"config": true,
		"admin":  true,
		"local":  true,
	}
	filter := bson.D{}
	dbs, err := c.client.ListDatabaseNames(ctx, filter)
	if err != nil {
		return nil, err
	}
	var filteredDbNames []string
	for _, db := range dbs {
		if _, ok := excludedDatabaseSet[db]; !ok {
			filteredDbNames = append(filteredDbNames, db)
		}
	}

	return filteredDbNames, nil
}
