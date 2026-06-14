package internal

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// Conversions between common.QualifiedTable and its proto counterpart live here
// because flow/pkg is a separate module that cannot depend on generated protos.

func QualifiedTableProto(t common.QualifiedTable) *protos.QualifiedTable {
	return &protos.QualifiedTable{Namespace: t.Namespace, Table: t.Table}
}

func QualifiedTableFromProto(t *protos.QualifiedTable) common.QualifiedTable {
	if t == nil {
		return common.QualifiedTable{}
	}
	return common.QualifiedTable{Namespace: t.Namespace, Table: t.Table}
}
