// TODO: This file contains temporary validation code used during the migration from v1 to v2 schema delta approaches.
// Once the v2 approach is fully rolled out and v1 is removed, this entire file should be deleted.
// The validation ensures that v2 produces identical results to v1 during the transition period.
//
// Related cleanup tasks when deleting this file:
// - Remove applySchemaDeltasV1() function once v1 is deprecated
// - Clean up any references to this validation logic
// - Clean up `PEERDB_APPLY_SCHEMA_DELTA_TO_CATALOG` in dynconf.go
package activities

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// validateV2AgainstV1 compares v1 and v2 schema delta approaches for validation.
// Failures in this validation are logged but don't affect production behavior.
func validateV2AgainstV1(
	ctx context.Context,
	pool shared.CatalogPool,
	flowName string,
	baseSchema map[string]*protos.TableSchema,
	schemaDeltas []*protos.TableSchemaDelta,
	affectedTables []string,
) {
	logger := internal.LoggerFromCtx(ctx)

	schemaAfterV1, err := internal.LoadTableSchemasFromCatalog(ctx, pool, flowName, affectedTables)
	if err != nil {
		logger.Warn("skipping v1/v2 validation: cannot load post-v1 schemas", slog.Any("error", err))
		return
	}

	schemaAfterV2, err := applySchemaDeltaV2(ctx, baseSchema, schemaDeltas)
	if err != nil {
		logger.Warn("skipping v1/v2 validation: applySchemaDeltaV2 failed", slog.Any("error", err))
		return
	}

	reportUnexpectedSchemaDiffs(ctx, flowName, baseSchema, schemaAfterV1, schemaAfterV2, schemaDeltas)
}

func reportUnexpectedSchemaDiffs(
	ctx context.Context,
	flowName string,
	baseSchemas map[string]*protos.TableSchema,
	schemasAfterV1 map[string]*protos.TableSchema,
	schemasAfterV2 map[string]*protos.TableSchema,
	schemaDeltas []*protos.TableSchemaDelta,
) {
	logger := internal.LoggerFromCtx(ctx)

	for _, schemaDelta := range schemaDeltas {
		tableName := schemaDelta.DstTableName
		baseSchema := baseSchemas[tableName]
		schemaV1 := schemasAfterV1[tableName]
		schemaV2 := schemasAfterV2[tableName]

		if baseSchema == nil || schemaV1 == nil || schemaV2 == nil {
			logger.Warn("skipping validation for table due to missing schema",
				slog.String("table", tableName),
				slog.Bool("hasBase", baseSchema != nil),
				slog.Bool("hasV1", schemaV1 != nil),
				slog.Bool("hasV2", schemaV2 != nil))
			continue
		}

		var issues []string

		// Build column maps for comparison
		baseColumnMap := make(map[string]*protos.FieldDescription)
		for _, col := range baseSchema.Columns {
			baseColumnMap[col.Name] = col
		}

		v1ColumnMap := make(map[string]*protos.FieldDescription)
		for _, col := range schemaV1.Columns {
			v1ColumnMap[col.Name] = col
		}

		v2ColumnMap := make(map[string]*protos.FieldDescription)
		for _, col := range schemaV2.Columns {
			v2ColumnMap[col.Name] = col
		}

		// Validate existing columns match in v1 and v2
		for _, baseCol := range baseSchema.Columns {
			v1Col, existsInV1 := v1ColumnMap[baseCol.Name]
			v2Col, existsInV2 := v2ColumnMap[baseCol.Name]

			if !existsInV1 {
				issues = append(issues, fmt.Sprintf("existing column '%s' missing in v1", baseCol.Name))
			}
			if !existsInV2 {
				issues = append(issues, fmt.Sprintf("existing column '%s' missing in v2", baseCol.Name))
			}

			if existsInV1 && existsInV2 && !fieldDescriptionEqual(v1Col, v2Col) {
				issues = append(issues, fmt.Sprintf("existing column '%s' differs: v1=%+v, v2=%+v",
					baseCol.Name, v1Col, v2Col))
			}
		}

		// Validate new columns match in v1 and v2
		for _, newCol := range schemaDelta.AddedColumns {
			v1Col, existsInV1 := v1ColumnMap[newCol.Name]
			v2Col, existsInV2 := v2ColumnMap[newCol.Name]

			if !existsInV1 {
				issues = append(issues, fmt.Sprintf("new column '%s' missing in v1", newCol.Name))
			}
			if !existsInV2 {
				issues = append(issues, fmt.Sprintf("new column '%s' missing in v2", newCol.Name))
			}

			if existsInV1 && existsInV2 && !fieldDescriptionEqual(v1Col, v2Col) {
				issues = append(issues, fmt.Sprintf("new column '%s' differs: v1=%+v, v2=%+v",
					newCol.Name, v1Col, v2Col))
			}
		}

		// Note: v1 may have extra columns due to race condition (fetching latest schema from source)
		// This is expected and not considered an error.

		// Check all other TableSchema fields match
		if schemaV1.TableIdentifier != schemaV2.TableIdentifier {
			issues = append(issues, fmt.Sprintf("table_identifier differs: v1='%s', v2='%s'",
				schemaV1.TableIdentifier, schemaV2.TableIdentifier))
		}
		if !slices.Equal(schemaV1.PrimaryKeyColumns, schemaV2.PrimaryKeyColumns) {
			issues = append(issues, fmt.Sprintf("primary_key_columns differs: v1=%v, v2=%v",
				schemaV1.PrimaryKeyColumns, schemaV2.PrimaryKeyColumns))
		}
		if schemaV1.IsReplicaIdentityFull != schemaV2.IsReplicaIdentityFull {
			issues = append(issues, fmt.Sprintf("is_replica_identity_full differs: v1=%v, v2=%v",
				schemaV1.IsReplicaIdentityFull, schemaV2.IsReplicaIdentityFull))
		}
		if schemaV1.System != schemaV2.System {
			issues = append(issues, fmt.Sprintf("system differs: v1=%v, v2=%v",
				schemaV1.System, schemaV2.System))
		}
		if schemaV1.NullableEnabled != schemaV2.NullableEnabled {
			issues = append(issues, fmt.Sprintf("nullable_enabled differs: v1=%v, v2=%v",
				schemaV1.NullableEnabled, schemaV2.NullableEnabled))
		}
		if schemaV1.TableOid != schemaV2.TableOid {
			issues = append(issues, fmt.Sprintf("table_oid differs: v1=%v, v2=%v",
				schemaV1.TableOid, schemaV2.TableOid))
		}

		// summarize the findings in logs
		if len(issues) > 0 {
			logger.Warn("schema validation issues found: v1 and v2 differ",
				slog.String("flowName", flowName),
				slog.String("table", tableName),
				slog.Int("issueCount", len(issues)),
				slog.Any("issues", issues))

			// if there is a discrepancy, report this as a metric to code_notification
			otel_metrics.CodeNotificationCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String("message", fmt.Sprintf("Schema delta v2 validation failed for flow=%s table=%s: %d issues",
					flowName, tableName, len(issues))),
				attribute.String("flowName", flowName),
				attribute.String("tableName", tableName),
			)))
		}
	}
}

func fieldDescriptionEqual(a, b *protos.FieldDescription) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Name == b.Name &&
		a.Type == b.Type &&
		a.TypeModifier == b.TypeModifier &&
		a.Nullable == b.Nullable
}
