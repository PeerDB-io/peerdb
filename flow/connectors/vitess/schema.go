package connvitess

import (
	"context"
	//"slices"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *VitessConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	_, err := c.Execute(ctx, "SHOW SCHEMAS")
	if err != nil {
		return nil, err
	}

	return nil, nil
	/*
		schemas := make([]string, 0, rs.RowNumber())
		for idx := range rs.RowNumber() {
			val, err := rs.GetString(idx, 0)
			if err != nil {
				return nil, err
			}
			if !slices.Contains([]string{"information_schema", "performance_schema", "sys"}, val) {
				schemas = append(schemas, val)
			}
		}
		return &protos.PeerSchemasResponse{Schemas: schemas}, nil
	*/
}
