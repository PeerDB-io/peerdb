package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type BuildInfo struct {
	Version string `bson:"version"`
}

func GetBuildInfo(ctx context.Context, client *mongo.Client) (BuildInfo, error) {
	return runCommand[BuildInfo](ctx, client, "buildInfo")
}

type OplogTruncation struct {
	OplogMinRetentionHours float64 `bson:"oplogMinRetentionHours"`
}

type StorageEngine struct {
	Name string `bson:"name"`
}

type ServerStatus struct {
	StorageEngine   StorageEngine   `bson:"storageEngine"`
	OplogTruncation OplogTruncation `bson:"oplogTruncation"`
	Host            string          `bson:"host"`
}

func GetServerStatus(ctx context.Context, client *mongo.Client) (ServerStatus, error) {
	return runCommand[ServerStatus](ctx, client, "serverStatus")
}

type ConnectionStatus struct {
	AuthInfo AuthInfo `bson:"authInfo"`
}

type AuthInfo struct {
	AuthenticatedUserRoles []Role `bson:"authenticatedUserRoles"`
}

type Role struct {
	Role string `bson:"role"`
	DB   string `bson:"db"`
}

func GetConnectionStatus(ctx context.Context, client *mongo.Client) (ConnectionStatus, error) {
	return runCommand[ConnectionStatus](ctx, client, "connectionStatus")
}

type HelloResponse struct {
	Msg   string   `bson:"msg,omitempty"`
	Hosts []string `bson:"hosts,omitempty"`
}

func GetHelloResponse(ctx context.Context, client *mongo.Client) (HelloResponse, error) {
	return runCommand[HelloResponse](ctx, client, "hello")
}

type CollStats struct {
	// uncompressed
	Size int64 `bson:"size"`
	// compressed
	StorageSize int64 `bson:"storageSize"`
}

func GetCollStats(ctx context.Context, client *mongo.Client, database string, collection string) (CollStats, error) {
	return runDatabaseCommand[CollStats](ctx, client, database, bson.D{
		{Key: "collStats", Value: collection},
		{Key: "scale", Value: 1},
	})
}

func runCommand[T any](ctx context.Context, client *mongo.Client, command string) (T, error) {
	var result T
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: command, Value: 1},
	})
	if singleResult.Err() != nil {
		return result, fmt.Errorf("'%s' failed: %v", command, singleResult.Err())
	}

	if err := singleResult.Decode(&result); err != nil {
		return result, fmt.Errorf("'%s' decoding failed: %v", command, err)
	}
	return result, nil
}

func runDatabaseCommand[T any](ctx context.Context, client *mongo.Client, database string, commandDoc bson.D) (T, error) {
	var result T
	singleResult := client.Database(database).RunCommand(ctx, commandDoc)
	if singleResult.Err() != nil {
		return result, fmt.Errorf("'%s' failed: %v", commandDoc.String(), singleResult.Err())
	}

	if err := singleResult.Decode(&result); err != nil {
		return result, fmt.Errorf("'%s' decoding failed: %v", commandDoc.String(), err)
	}
	return result, nil
}
