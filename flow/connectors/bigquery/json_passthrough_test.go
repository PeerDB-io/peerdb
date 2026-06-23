package connbigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func testQRecordSchema(columnName string, kind types.QValueKind) types.QRecordSchema {
	return types.QRecordSchema{
		Fields: []types.QField{{Name: columnName, Type: kind}},
	}
}

func testProtoTableSchema(columnName string, kind types.QValueKind) *protos.TableSchema {
	return &protos.TableSchema{
		Columns: []*protos.FieldDescription{{Name: columnName, Type: string(kind)}},
	}
}

func testBigQuerySchema(columnName string, fieldType bigquery.FieldType) bigquery.Schema {
	return bigquery.Schema{
		&bigquery.FieldSchema{Name: columnName, Type: fieldType},
	}
}

func TestValidateQRepJSONPassthroughColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		columns   []string
		srcSchema types.QRecordSchema
		dstSchema bigquery.Schema
		wantErr   string
	}{
		{
			name:      "allows scalar json",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindJSON),
			dstSchema: testBigQuerySchema("payload", bigquery.JSONFieldType),
		},
		{
			name:      "allows scalar jsonb",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindJSONB),
			dstSchema: testBigQuerySchema("payload", bigquery.JSONFieldType),
		},
		{
			name:      "ignores empty config",
			srcSchema: testQRecordSchema("payload", types.QValueKindString),
			dstSchema: testBigQuerySchema("payload", bigquery.StringFieldType),
		},
		{
			name:      "rejects source json array",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindArrayJSON),
			dstSchema: testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:   "unsupported JSON array source type",
		},
		{
			name:      "rejects non JSON source",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindString),
			dstSchema: testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:   "non-JSON source type",
		},
		{
			name:      "rejects missing source column",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("other_payload", types.QValueKindJSON),
			dstSchema: testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:   "was not found in source schema",
		},
		{
			name:      "rejects missing destination column",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindJSON),
			dstSchema: testBigQuerySchema("other_payload", bigquery.JSONFieldType),
			wantErr:   "was not found in BigQuery destination schema",
		},
		{
			name:      "rejects non JSON destination",
			columns:   []string{"payload"},
			srcSchema: testQRecordSchema("payload", types.QValueKindJSON),
			dstSchema: testBigQuerySchema("payload", bigquery.StringFieldType),
			wantErr:   "non-JSON BigQuery destination type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateQRepJSONPassthroughColumns(tt.columns, tt.srcSchema, tt.dstSchema)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidateCDCJSONPassthroughColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		columns     []string
		tableSchema *protos.TableSchema
		dstSchema   bigquery.Schema
		wantErr     string
	}{
		{
			name:        "allows scalar json with BigQuery JSON destination",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindJSON),
			dstSchema:   testBigQuerySchema("payload", bigquery.JSONFieldType),
		},
		{
			name:        "allows scalar jsonb with BigQuery JSON destination",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindJSONB),
			dstSchema:   testBigQuerySchema("payload", bigquery.JSONFieldType),
		},
		{
			name:        "ignores empty config",
			tableSchema: testProtoTableSchema("payload", types.QValueKindString),
			dstSchema:   testBigQuerySchema("payload", bigquery.StringFieldType),
		},
		{
			name:        "rejects missing table schema",
			columns:     []string{"payload"},
			tableSchema: nil,
			dstSchema:   testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:     "table schema was not found",
		},
		{
			name:        "rejects source json array",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindArrayJSON),
			dstSchema:   testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:     "unsupported JSON array source type",
		},
		{
			name:        "rejects non JSON source",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindString),
			dstSchema:   testBigQuerySchema("payload", bigquery.JSONFieldType),
			wantErr:     "non-JSON source type",
		},
		{
			name:        "rejects missing destination column",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindJSON),
			dstSchema:   testBigQuerySchema("other_payload", bigquery.JSONFieldType),
			wantErr:     "was not found in BigQuery destination schema",
		},
		{
			name:        "rejects non JSON destination",
			columns:     []string{"payload"},
			tableSchema: testProtoTableSchema("payload", types.QValueKindJSON),
			dstSchema:   testBigQuerySchema("payload", bigquery.StringFieldType),
			wantErr:     "non-JSON BigQuery destination type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateCDCJSONPassthroughColumns("dataset.events", tt.columns, tt.tableSchema, tt.dstSchema)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
