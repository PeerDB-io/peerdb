package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	MinSupportedVersion    = "5.1.0"
	MinOplogRetentionHours = 24
)

type BuildInfo struct {
	Version string `bson:"version"`
}

type ReplSetGetStatus struct {
	Set     string `bson:"set"`
	MyState int    `bson:"myState"`
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
}

func GetBuildInfo(ctx context.Context, client *mongo.Client) (*BuildInfo, error) {
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{bson.E{Key: "buildInfo", Value: 1}})
	if singleResult.Err() != nil {
		return nil, fmt.Errorf("failed to run 'buildInfo' command: %w", singleResult.Err())
	}
	var info BuildInfo
	if err := singleResult.Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode BuildInfo: %w", err)
	}
	return &info, nil
}

func GetReplSetGetStatus(ctx context.Context, client *mongo.Client) (*ReplSetGetStatus, error) {
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: "replSetGetStatus", Value: 1},
	})
	if singleResult.Err() != nil {
		return nil, fmt.Errorf("failed to run 'replSetGetStatus' command: %w", singleResult.Err())
	}
	var status ReplSetGetStatus
	if err := singleResult.Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode ReplSetGetStatus: %w", err)
	}
	return &status, nil
}

func GetServerStatus(ctx context.Context, client *mongo.Client) (*ServerStatus, error) {
	singleResult := client.Database("admin").RunCommand(ctx, bson.D{
		bson.E{Key: "serverStatus", Value: 1},
	})
	if singleResult.Err() != nil {
		return nil, fmt.Errorf("failed to run 'serverStatus' command: %w", singleResult.Err())
	}
	var status ServerStatus
	if err := singleResult.Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode ServerStatus: %w", err)
	}
	return &status, nil
}
