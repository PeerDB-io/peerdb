package internal

import (
	"context"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// SourceSchemaColumnName is the column PEERDB_SOURCE_SCHEMA_AS_DESTINATION_COLUMN adds to every replicated row.
const SourceSchemaColumnName = "_peerdb_source_schema"

// SnapshotProjectsSourceSchema reports whether a snapshot's rows carry SourceSchemaColumnName, computed by the
// Postgres source per row, instead of the destination stamping the mirrored table's schema as a literal.
// The source and the destination both evaluate it from the QRepConfig alone, so they always agree.
// It depends on exactly these fields of the config: Env, Version, Query and SourceType.
func SnapshotProjectsSourceSchema(ctx context.Context, config *protos.QRepConfig) (bool, error) {
	if config.Query != "" || config.SourceType != protos.DBType_POSTGRES ||
		config.Version < shared.InternalVersion_SourceSchemaFromInheritanceChild {
		return false, nil
	}
	return PeerDBSourceSchemaAsDestinationColumn(ctx, config.Env)
}
