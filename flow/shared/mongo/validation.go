package mongo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	MinSupportedVersion    = "5.1.0"
	MinOplogRetentionHours = 24

	ReplicaSet     = "ReplicaSet"
	ShardedCluster = "ShardedCluster"
)

var RequiredRoles = [...]string{"readAnyDatabase", "clusterMonitor"}

type Credentials struct {
	RootCa     *string
	Username   string
	Password   string
	TlsHost    string
	DisableTls bool
}

func ValidateServerCompatibility(ctx context.Context, client *mongo.Client, credentials Credentials) error {
	buildInfo, err := GetBuildInfo(ctx, client)
	if err != nil {
		return err
	}

	if cmp, err := CompareServerVersions(buildInfo.Version, MinSupportedVersion); err != nil {
		return err
	} else if cmp < 0 {
		return fmt.Errorf("require minimum mongo version %s", MinSupportedVersion)
	}

	validateStorageEngine := func(instanceCtx context.Context, instanceClient *mongo.Client) error {
		ss, err := GetServerStatus(instanceCtx, instanceClient)
		if err != nil {
			return err
		}

		if ss.StorageEngine.Name != "wiredTiger" {
			return errors.New("only wiredTiger storage engine is supported")
		}
		return nil
	}

	topologyType, err := GetTopologyType(ctx, client)
	if err != nil {
		return err
	}

	if topologyType == ReplicaSet {
		return validateStorageEngine(ctx, client)
	} else {
		return runOnShardsInParallel(ctx, client, credentials, validateStorageEngine)
	}
}

func ValidateUserRoles(ctx context.Context, client *mongo.Client) error {
	connectionStatus, err := GetConnectionStatus(ctx, client)
	if err != nil {
		return err
	}

	for _, requiredRole := range RequiredRoles {
		if !slices.ContainsFunc(connectionStatus.AuthInfo.AuthenticatedUserRoles, func(r Role) bool {
			return r.Role == requiredRole
		}) {
			return fmt.Errorf("missing required role: %s", requiredRole)
		}
	}

	return nil
}

func ValidateOplogRetention(ctx context.Context, client *mongo.Client, credentials Credentials) error {
	validateOplogRetention := func(instanceCtx context.Context, instanceClient *mongo.Client) error {
		ss, err := GetServerStatus(instanceCtx, instanceClient)
		if err != nil {
			return err
		}
		if ss.OplogTruncation.OplogMinRetentionHours == 0 ||
			ss.OplogTruncation.OplogMinRetentionHours < MinOplogRetentionHours {
			return fmt.Errorf("oplog retention must be set to >= 24 hours, but got %f",
				ss.OplogTruncation.OplogMinRetentionHours)
		}
		return nil
	}

	topology, err := GetTopologyType(ctx, client)
	if err != nil {
		return err
	}
	if topology == ReplicaSet {
		return validateOplogRetention(ctx, client)
	} else {
		return runOnShardsInParallel(ctx, client, credentials, validateOplogRetention)
	}
}

func GetTopologyType(ctx context.Context, client *mongo.Client) (string, error) {
	hello, err := GetHelloResponse(ctx, client)
	if err != nil {
		return "", err
	}

	// Only replica set has 'hosts' field
	// https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.hosts
	if len(hello.Hosts) > 0 {
		return ReplicaSet, nil
	}

	// Only sharded cluster has 'msg' field, and equals to 'isdbgrid'
	// https://www.mongodb.com/docs/manual/reference/command/hello/#mongodb-data-hello.msg
	if hello.Msg == "isdbgrid" {
		return ShardedCluster, nil
	}
	return "", errors.New("topology type must be ReplicaSet or ShardedCluster")
}

func runOnShardsInParallel(
	ctx context.Context,
	client *mongo.Client,
	credentials Credentials,
	runCommand func(ctx context.Context, client *mongo.Client) error,
) error {
	res, err := GetListShards(ctx, client)
	if err != nil {
		return err
	}

	if res.Ok != 1 || len(res.Shards) == 0 {
		return errors.New("invalid shards")
	}

	hosts := getUniqueClusterHosts(res.Shards)
	hostsErrors := make([]error, len(hosts))
	var wg sync.WaitGroup
	for idx, host := range hosts {
		wg.Add(1)
		go func(i int, h string) {
			defer wg.Done()

			shardOpts := options.Client().
				ApplyURI("mongodb://" + h).
				SetDirect(true).
				SetAuth(options.Credential{
					Username: credentials.Username,
					Password: credentials.Password,
				})

			if !credentials.DisableTls {
				tlsConfig, err := shared.CreateTlsConfig(tls.VersionTLS12, credentials.RootCa, "", credentials.TlsHost, false)
				if err != nil {
					hostsErrors[i] = fmt.Errorf("host %s TLS config error: %w", h, err)
					return
				}
				shardOpts.SetTLSConfig(tlsConfig)
			}

			shardClient, err := mongo.Connect(shardOpts)
			if err != nil {
				hostsErrors[i] = fmt.Errorf("host %s connect error: %w", h, err)
				return
			}
			defer shardClient.Disconnect(ctx) //nolint:errcheck

			if err := runCommand(ctx, shardClient); err != nil {
				hostsErrors[i] = fmt.Errorf("host %s command error: %w", h, err)
				return
			}
		}(idx, host)
	}
	wg.Wait()

	for _, err = range hostsErrors {
		if err != nil {
			return err
		}
	}

	return nil
}

func getUniqueClusterHosts(shards []Shard) []string {
	hostSet := make(map[string]bool)
	for _, shard := range shards {
		hosts := shard.Host
		if slashIdx := strings.Index(hosts, "/"); slashIdx != -1 {
			hosts = hosts[slashIdx+1:]
		}

		for _, host := range strings.Split(hosts, ",") {
			host = strings.TrimSpace(host)
			if host != "" {
				hostSet[host] = true
			}
		}
	}

	hosts := make([]string, 0, len(hostSet))
	for host := range hostSet {
		hosts = append(hosts, host)
	}

	return hosts
}
