package main

import (
	"context"
	"database/sql"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func (h *FlowRequestHandler) getPoolForPGPeer(ctx context.Context, peerName string) (*pgxpool.Pool, string, error) {
	var pgPeerOptions sql.RawBytes
	var pgPeerConfig protos.PostgresConfig
	err := h.pool.QueryRow(ctx,
		"SELECT options FROM peers WHERE name = $1 AND type=3", peerName).Scan(&pgPeerOptions)
	if err != nil {
		return nil, "", err
	}

	unmarshalErr := proto.Unmarshal(pgPeerOptions, &pgPeerConfig)
	if err != nil {
		return nil, "", unmarshalErr
	}

	connStr := utils.GetPGConnectionString(&pgPeerConfig)
	peerPool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, "", unmarshalErr
	}
	return peerPool, pgPeerConfig.User, nil
}

func (h *FlowRequestHandler) GetSlotInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerSlotResponse, error) {
	peerPool, _, err := h.getPoolForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerSlotResponse{SlotData: nil}, err
	}
	defer peerPool.Close()
	rows, err := peerPool.Query(ctx, "SELECT slot_name, redo_lsn::Text,restart_lsn::text,active,"+
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
		var active bool
		var lagInMB float32
		err := rows.Scan(&slotName, &redoLSN, &restartLSN, &active, &lagInMB)
		if err != nil {
			return &protos.PeerSlotResponse{SlotData: nil}, err
		}

		slotInfoRows = append(slotInfoRows, &protos.SlotInfo{
			RedoLSN:    redoLSN,
			RestartLSN: restartLSN,
			SlotName:   slotName,
			Active:     active,
			LagInMb:    lagInMB,
		})
	}
	return &protos.PeerSlotResponse{
		SlotData: slotInfoRows,
	}, nil
}

func (h *FlowRequestHandler) GetStatInfo(
	ctx context.Context,
	req *protos.PostgresPeerActivityInfoRequest,
) (*protos.PeerStatResponse, error) {
	peerPool, peerUser, err := h.getPoolForPGPeer(ctx, req.PeerName)
	if err != nil {
		return &protos.PeerStatResponse{StatData: nil}, err
	}
	defer peerPool.Close()
	rows, err := peerPool.Query(ctx, "SELECT pid, wait_event, wait_event_type, query_start::text, query,"+
		"EXTRACT(epoch FROM(now()-query_start)) AS dur"+
		" FROM pg_stat_activity WHERE usename=$1 AND state != 'idle' AND query_start IS NOT NULL;", peerUser)
	if err != nil {
		return &protos.PeerStatResponse{StatData: nil}, err
	}
	defer rows.Close()
	var statInfoRows []*protos.StatInfo
	for rows.Next() {
		var pid int64
		var waitEvent string
		var waitEventType string
		var queryStart string
		var query string
		var duration float32

		err := rows.Scan(&pid, &waitEvent, &waitEventType, &queryStart, &query, &duration)
		if err != nil {
			return &protos.PeerStatResponse{StatData: nil}, err
		}

		statInfoRows = append(statInfoRows, &protos.StatInfo{
			Pid:           pid,
			WaitEvent:     waitEvent,
			WaitEventType: waitEventType,
			QueryStart:    queryStart,
			Query:         query,
			Duration:      duration,
		})
	}
	return &protos.PeerStatResponse{
		StatData: statInfoRows,
	}, nil
}
