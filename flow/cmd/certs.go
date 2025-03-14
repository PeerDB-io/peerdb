package cmd

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (h *FlowRequestHandler) GetCerts(ctx context.Context, req *protos.GetCertsRequest) (*protos.GetCertsResponse, error) {
	whereClause := ""
	if req.Id != -1 {
		whereClause = fmt.Sprintf(" WHERE id=%d", req.Id)
	}
	rows, err := h.pool.Query(ctx, "SELECT id,name,source FROM certs"+whereClause)
	if err != nil {
		return nil, err
	}

	certs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*protos.Cert, error) {
		cert := &protos.Cert{}
		var sourceBytes []byte
		err := row.Scan(&cert.Id, &cert.Name, &sourceBytes)
		if err == nil {
			cert.Source = string(sourceBytes)
		}
		return cert, err
	})
	if err != nil {
		return nil, err
	}

	return &protos.GetCertsResponse{Certs: certs}, nil
}

func (h *FlowRequestHandler) PostCert(ctx context.Context, req *protos.PostCertRequest) (*protos.PostCertResponse, error) {
	if !x509.NewCertPool().AppendCertsFromPEM(shared.UnsafeFastStringToReadOnlyBytes(req.Cert.Source)) {
		return nil, errors.New("invalid file, expecting PEM certificates")
	}

	if req.Cert.Id == -1 {
		var id int32
		if err := h.pool.QueryRow(
			ctx,
			"INSERT INTO certs(name,source) VALUES($1,$2) RETURNING id",
			req.Cert.Name,
			[]byte(req.Cert.Source),
		).Scan(&id); err != nil {
			return nil, err
		}
		return &protos.PostCertResponse{Id: id}, nil
	} else if _, err := h.pool.Exec(
		ctx,
		"UPDATE certs SET name=$1,source=$2 where id=$3",
		req.Cert.Name,
		[]byte(req.Cert.Source),
		req.Cert.Id,
	); err != nil {
		return nil, err
	}
	return &protos.PostCertResponse{Id: req.Cert.Id}, nil
}

func (h *FlowRequestHandler) DeleteCert(
	ctx context.Context,
	req *protos.DeleteCertRequest,
) (*protos.DeleteCertResponse, error) {
	if _, err := h.pool.Exec(ctx, "DELETE FROM certs WHERE id=$1", req.Id); err != nil {
		return nil, err
	}
	return &protos.DeleteCertResponse{}, nil
}
