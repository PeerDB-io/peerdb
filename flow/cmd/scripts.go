package cmd

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (h *FlowRequestHandler) GetScripts(ctx context.Context, req *protos.GetScriptsRequest) (*protos.GetScriptsResponse, error) {
	whereClause := ""
	if req.Id != -1 {
		whereClause = fmt.Sprintf(" WHERE id=%d", req.Id)
	}
	rows, err := h.pool.Query(ctx, "SELECT id,lang,name,source FROM scripts"+whereClause)
	if err != nil {
		return nil, err
	}

	scripts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.Script, error) {
		script := &protos.Script{}
		var sourceBytes []byte
		err := row.Scan(&script.Id, &script.Lang, &script.Name, &sourceBytes)
		if err == nil {
			script.Source = string(sourceBytes)
		}
		return script, err
	})
	if err != nil {
		return nil, err
	}

	return &protos.GetScriptsResponse{Scripts: scripts}, nil
}

func (h *FlowRequestHandler) PostScript(ctx context.Context, req *protos.PostScriptRequest) (*protos.PostScriptResponse, error) {
	if req.Script.Id == -1 {
		var id int32
		if err := h.pool.QueryRow(
			ctx,
			"INSERT INTO scripts(lang,name,source) VALUES($1,$2,$3) RETURNING id",
			req.Script.Lang,
			req.Script.Name,
			[]byte(req.Script.Source),
		).Scan(&id); err != nil {
			return nil, err
		}
		return &protos.PostScriptResponse{Id: id}, nil
	} else if _, err := h.pool.Exec(
		ctx,
		"UPDATE scripts SET lang=$1,name=$2,source=$3 where id=$4",
		req.Script.Lang,
		req.Script.Name,
		[]byte(req.Script.Source),
		req.Script.Id,
	); err != nil {
		return nil, err
	}
	return &protos.PostScriptResponse{Id: req.Script.Id}, nil
}

func (h *FlowRequestHandler) DeleteScript(
	ctx context.Context,
	req *protos.DeleteScriptRequest,
) (*protos.DeleteScriptResponse, error) {
	if _, err := h.pool.Exec(ctx, "DELETE FROM scripts WHERE id=$1", req.Id); err != nil {
		return nil, err
	}
	return &protos.DeleteScriptResponse{}, nil
}
