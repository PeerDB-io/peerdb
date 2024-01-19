package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (h *FlowRequestHandler) GetPostgresVersion(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	if pool == nil {
		return -1, fmt.Errorf("version check: pool is nil")
	}

	var versionRes string
	err := pool.QueryRow(ctx, "SHOW server_version_num;").Scan(&versionRes)
	if err != nil {
		return -1, err
	}

	version, err := strconv.Atoi(versionRes)
	if err != nil {
		return -1, err
	}

	return version, nil
}

func (h *FlowRequestHandler) CheckReplicationPermissions(ctx context.Context, pool *pgxpool.Pool, username string) error {
	if pool == nil {
		return fmt.Errorf("check replication permissions: pool is nil")
	}

	var replicationRes bool
	err := pool.QueryRow(ctx, "SELECT rolreplication FROM pg_roles WHERE rolname = $1;", username).Scan(&replicationRes)
	if err != nil {
		return err
	}

	if !replicationRes {
		return fmt.Errorf("postgres user does not have replication role")
	}

	// check wal_level
	var walLevel string
	err = pool.QueryRow(ctx, "SHOW wal_level;").Scan(&walLevel)
	if err != nil {
		return err
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level is not logical")
	}

	// max_wal_senders must be at least 2
	var maxWalSendersRes string
	err = pool.QueryRow(ctx, "SHOW max_wal_senders;").Scan(&maxWalSendersRes)
	if err != nil {
		return err
	}

	maxWalSenders, err := strconv.Atoi(maxWalSendersRes)
	if err != nil {
		return err
	}

	if maxWalSenders < 2 {
		return fmt.Errorf("max_wal_senders must be at least 1")
	}

	return nil
}

func (h *FlowRequestHandler) CheckSourceTables(ctx context.Context, pool *pgxpool.Pool, tableNames []string, pubName string) error {
	if pool == nil {
		return fmt.Errorf("check tables: pool is nil")
	}

	// Check that we can select from all tables
	for _, tableName := range tableNames {
		var row pgx.Row
		err := pool.QueryRow(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0;", tableName)).Scan(&row)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
	}

	// Check if tables belong to publication
	tableArr := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		tableArr = append(tableArr, fmt.Sprintf("'%s'", tableName))
	}

	tableStr := strings.Join(tableArr, ",")

	if pubName != "" {
		var pubTableCount int
		err := pool.QueryRow(ctx, fmt.Sprintf("select COUNT(DISTINCT(schemaname||'.'||tablename)) from pg_publication_tables "+
			"where schemaname||'.'||tablename in (%s) and pubname=$1;", tableStr), pubName).Scan(&pubTableCount)
		if err != nil {
			return err
		}

		if pubTableCount != len(tableNames) {
			return fmt.Errorf("not all tables belong to publication")
		}
	}

	return nil
}

func (h *FlowRequestHandler) ValidateCDCMirror(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.ValidateCDCMirrorResponse, error) {
	sourcePeerName := req.ConnectionConfigs.Source.Name
	sourcePool, err := h.getPoolForPGPeer(ctx, sourcePeerName)
	if err != nil {
		slog.Error("/validatecdc failed to obtain peer connection", slog.Any("error", err))
		return nil, err
	}

	// 1. Deny PG version < 12
	version, err := h.GetPostgresVersion(ctx, sourcePool)
	if err != nil {
		slog.Error("/validatecdc pg version check", slog.Any("error", err))
		return nil, err
	}

	if version < 12 {
		return &protos.ValidateCDCMirrorResponse{
				Ok: false,
			}, fmt.Errorf("postgres version %d is not supported. "+
				"Please upgrade to Postgres 12 or higher", version)
	}

	sourcePeerConfig := req.ConnectionConfigs.Source.GetPostgresConfig()
	if sourcePeerConfig == nil {
		slog.Error("/validatecdc source peer config is nil", slog.Any("peer", req.ConnectionConfigs.Source))
		return nil, fmt.Errorf("source peer config is nil")
	}

	// 2. Check permissions of postgres peer
	err = h.CheckReplicationPermissions(ctx, sourcePool, sourcePeerConfig.User)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("failed to check replication permissions: %v", err)
	}

	// 3. Check source tables
	sourceTables := make([]string, 0, len(req.ConnectionConfigs.TableMappings))
	for _, tableMapping := range req.ConnectionConfigs.TableMappings {
		sourceTables = append(sourceTables, tableMapping.SourceTableIdentifier)
	}

	err = h.CheckSourceTables(ctx, sourcePool, sourceTables, req.ConnectionConfigs.PublicationName)
	if err != nil {
		return &protos.ValidateCDCMirrorResponse{
			Ok: false,
		}, fmt.Errorf("provided source tables invalidated: %v", err)
	}

	return &protos.ValidateCDCMirrorResponse{
		Ok: true,
	}, nil
}
