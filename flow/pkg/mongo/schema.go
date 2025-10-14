package mongo

import (
	"context"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Filter out system databases after listing all databases.
// Some MongoDB Atlas tiers (e.g., free tier) don't support regex filters in ListDatabases.
func GetDatabaseNames(ctx context.Context, client *mongo.Client) ([]string, error) {
	dbs, err := client.ListDatabases(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	filteredDbNames := make([]string, 0, len(dbs.Databases))
	for _, db := range dbs.Databases {
		if db.Name == "admin" || db.Name == "local" || db.Name == "config" {
			continue
		}
		filteredDbNames = append(filteredDbNames, db.Name)
	}
	slices.Sort(filteredDbNames)
	return filteredDbNames, nil
}

// Filter out views and system collections after listing all collections.
// Some MongoDB Atlas tiers (e.g., free tier) don't support regex filters in ListCollections.
func GetCollectionNames(ctx context.Context, client *mongo.Client, databaseName string) ([]string, error) {
	db := client.Database(databaseName)
	cur, err := db.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	type CollectionSpec struct {
		Name string `bson:"name"`
		Type string `bson:"type"` // "collection" | "view" | etc
	}

	filteredCollNames := make([]string, 0, 100)
	for cur.Next(ctx) {
		if err := cur.Err(); err != nil {
			return nil, err
		}

		var coll CollectionSpec
		if err := cur.Decode(&coll); err != nil {
			return nil, err
		}

		if strings.HasPrefix(coll.Name, "system.") {
			continue
		}
		if strings.EqualFold(coll.Type, "view") {
			continue
		}
		filteredCollNames = append(filteredCollNames, coll.Name)
	}
	slices.Sort(filteredCollNames)
	return filteredCollNames, nil
}
