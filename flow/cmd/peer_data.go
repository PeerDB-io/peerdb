package main

import (
	"context"
	"database/sql"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func (h *FlowRequestHandler) GetPoolForPGPeer(ctx context.Context, peerName string) (*pgxpool.Pool, error) {
	var pgPeerOptions sql.RawBytes
	var pgPeerConfig protos.PostgresConfig
	err := h.pool.QueryRow(ctx,
		"SELECT options FROM peers WHERE name = $1 AND type=3", peerName).Scan(&pgPeerOptions)
	if err != nil {
		return nil, err
	}

	unmarshalErr := proto.Unmarshal(pgPeerOptions, &pgPeerConfig)
	if err != nil {
		return nil, unmarshalErr
	}

	connStr := utils.GetPGConnectionString(&pgPeerConfig)
	peerPool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, unmarshalErr
	}
	return peerPool, nil
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PeerDataRequest,
) (*protos.PeerSlotResponse, error) {
	peerPool, err := h.GetPoolForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}
	defer peerPool.Close()
	rows, err := peerPool.Query(ctx, "SELECT slot_name, redo_lsn::Text,restart_lsn::text,"+
		"round((redo_lsn-restart_lsn) / 1024 / 1024 , 2) AS MB_Behind"+
		" FROM pg_control_checkpoint(), pg_replication_slots;")
	if err != nil {
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}
	defer rows.Close()
	var slotInfoRows []*protos.SlotInfo
	for rows.Next() {
		var redoLSN string
		var slotName string
		var restartLSN string
		var lagInMB float32
		err := rows.Scan(&slotName, &redoLSN, &restartLSN, &lagInMB)
		if err != nil {
			return &protos.PeerSlotResponse{SlotData: nil}, err
		}

		slotInfoRows = append(slotInfoRows, &protos.SlotInfo{
			RedoLSN:    redoLSN,
			RestartLSN: restartLSN,
			SlotName:   slotName,
			LagInMb:    lagInMB,
		})
	}
	return &protos.PeerSlotResponse{
		SlotData: slotInfoRows,
	}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PeerDataRequest,
) (*protos.PeerStatResponse, error) {
	peerPool, err := h.GetPoolForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerStatResponse{StatData: nil}, err
	}
	defer peerPool.Close()
	rows, err := peerPool.Query(ctx, "SELECT pid, query, EXTRACT(epoch FROM(now()-query_start)) AS dur"+
		" FROM pg_stat_activity WHERE query_start IS NOT NULL;")
	if err != nil {
		return &protos.PeerStatResponse{StatData: nil}, err
	}
	defer rows.Close()
	var statInfoRows []*protos.StatInfo
	for rows.Next() {
		var pid int64
		var query string
		var duration float32

		err := rows.Scan(&pid, &query, &duration)
		if err != nil {
			return &protos.PeerStatResponse{StatData: nil}, err
		}

		statInfoRows = append(statInfoRows, &protos.StatInfo{
			Pid:      pid,
			Query:    query,
			Duration: duration,
		})
	}
	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}
