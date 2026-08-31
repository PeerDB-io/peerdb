package connmongo

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// mongoProjection describes one user-configured typed column extracted from a
// BSON document. Path is the (possibly nested) source field path, DestName is
// the destination column, and Kind is the target QValueKind.
type mongoProjection struct {
	DestName string
	Kind     types.QValueKind
	Path     []string
}

// buildProjections turns per-table ColumnSetting entries into typed projections.
// A ColumnSetting is treated as a typed projection only when it carries an
// explicit destination_type; rename-only settings (no type) and settings that
// collide with the reserved _id/doc columns are ignored. source_name is the BSON
// field path in dot notation (e.g. "meta.addedSource.source").
func buildProjections(columns []*protos.ColumnSetting) []mongoProjection {
	if len(columns) == 0 {
		return nil
	}
	projections := make([]mongoProjection, 0, len(columns))
	for _, col := range columns {
		if col.SourceName == "" || col.DestinationType == "" {
			continue
		}
		dest := col.DestinationName
		if dest == "" {
			dest = col.SourceName
		}
		if dest == DefaultDocumentKeyColumnName ||
			dest == DefaultFullDocumentColumnName ||
			dest == LegacyFullDocumentColumnName {
			continue
		}
		projections = append(projections, mongoProjection{
			DestName: dest,
			Kind:     types.QValueKind(col.DestinationType),
			Path:     strings.Split(col.SourceName, "."),
		})
	}
	return projections
}

// projectedFieldDescriptions returns the FieldDescriptions to append to a Mongo
// table schema for the given projections. All are nullable since MongoDB is
// schemaless and a field may be absent or of an unexpected type.
func projectedFieldDescriptions(projections []mongoProjection) []*protos.FieldDescription {
	fields := make([]*protos.FieldDescription, 0, len(projections))
	for _, p := range projections {
		fields = append(fields, &protos.FieldDescription{
			Name:         p.DestName,
			Type:         string(p.Kind),
			TypeModifier: -1,
			Nullable:     true,
		})
	}
	return fields
}

// projectedQFields returns the QRecordSchema fields to append for the snapshot path.
func projectedQFields(projections []mongoProjection) []types.QField {
	fields := make([]types.QField, 0, len(projections))
	for _, p := range projections {
		fields = append(fields, types.QField{
			Name:     p.DestName,
			Type:     p.Kind,
			Nullable: true,
		})
	}
	return fields
}

// projectValue extracts a single projected column from a document. A missing
// field (or a nil/empty document, e.g. a delete event) yields a typed NULL. A
// value whose BSON type cannot be coerced to the target kind also yields NULL,
// so heterogeneous source data cannot stall replication.
func projectValue(raw bson.Raw, p mongoProjection, converter BsonToQValueConverter) (types.QValue, error) {
	if len(raw) == 0 {
		return types.QValueNull(p.Kind), nil
	}
	return converter.QValueFromBsonValue(raw.Lookup(p.Path...), p.Kind)
}

// appendProjectedValues appends the projected column values (in projection order)
// to values, used by the snapshot path which produces an ordered []QValue.
func appendProjectedValues(
	values []types.QValue,
	raw bson.Raw,
	projections []mongoProjection,
	converter BsonToQValueConverter,
) ([]types.QValue, error) {
	for _, p := range projections {
		qv, err := projectValue(raw, p, converter)
		if err != nil {
			return nil, err
		}
		values = append(values, qv)
	}
	return values, nil
}
