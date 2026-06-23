package connpostgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func newTestCDCSource(t *testing.T) *PostgresCDCSource {
	t.Helper()
	return &PostgresCDCSource{
		PostgresConnector: &PostgresConnector{typeMap: pgtype.NewMap()},
		jsonApi:           createExtendedJSONUnmarshaler(),
		otelManager:       &otel_metrics.OtelManager{},
	}
}

func cdcTestTableSchema(columnName string, kind types.QValueKind) *protos.TableSchema {
	return &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: columnName, Type: string(kind)},
		},
	}
}

func TestProcessMessageInvalidMessage(t *testing.T) {
	t.Parallel()

	p := newTestCDCSource(t)
	batch := model.NewCDCStream[model.RecordItems](0)

	xld := pglogrepl.XLogData{
		WALStart:     pglogrepl.LSN(0x01),
		ServerWALEnd: pglogrepl.LSN(0x02),
		// 'S' is a v2 Stream Start tag — not handled by pglogrepl.Parse (v1)
		WALData: []byte{'S', 0, 1, 0, 1 /*arbitrary bytes*/},
	}

	rec, err := processMessage(t.Context(), p, batch, xld, xld.WALStart, qProcessor{})
	require.Nil(t, rec)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error parsing logical message (msgType=\"S\", walStart=0/1)")
}

func TestCDCDecodeJSONPassthroughPreservesRawNestedJSON(t *testing.T) {
	t.Parallel()

	p := newTestCDCSource(t)
	rawJSON := `{"items":[{"n":1000000000000000000000000000000000000000}],"deep":{"array":[1,2,{"ok":true}]}}`

	qvalue, err := p.decodeColumnData(
		[]byte(rawJSON),
		pgtype.JSONOID,
		-1,
		pgtype.TextFormatCode,
		nil,
		0,
		true,
	)

	require.NoError(t, err)
	jsonValue, ok := qvalue.(types.QValueJSON)
	require.True(t, ok)
	require.Equal(t, rawJSON, jsonValue.Val)
	require.False(t, jsonValue.IsArray)
}

func TestValidateCDCJSONPassthroughConfig(t *testing.T) {
	t.Parallel()

	tableMapping := map[string]model.NameAndExclude{
		"public.events": model.NewNameAndExclude("dataset.events", nil, []string{"payload"}),
	}

	tests := []struct {
		name          string
		destination   protos.DBType
		schemaMapping map[string]*protos.TableSchema
		wantErr       string
	}{
		{
			name:        "allows scalar json",
			destination: protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindJSON),
			},
		},
		{
			name:        "allows scalar jsonb",
			destination: protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindJSONB),
			},
		},
		{
			name:        "rejects non BigQuery destination",
			destination: protos.DBType_POSTGRES,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindJSON),
			},
			wantErr: "non-BigQuery destination",
		},
		{
			name:        "rejects json array schema",
			destination: protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindArrayJSON),
			},
			wantErr: "unsupported JSON array type",
		},
		{
			name:        "rejects non JSON schema",
			destination: protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindString),
			},
			wantErr: "maps to non-JSON destination type",
		},
		{
			name:        "rejects missing column",
			destination: protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("other_payload", types.QValueKindJSON),
			},
			wantErr: "destination column was not found",
		},
		{
			name:          "rejects missing table schema",
			destination:   protos.DBType_BIGQUERY,
			schemaMapping: map[string]*protos.TableSchema{},
			wantErr:       "destination table schema was not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateCDCJSONPassthroughConfig(tt.destination, tableMapping, tt.schemaMapping)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestShouldPassthroughJSONColumnOIDValidation(t *testing.T) {
	t.Parallel()

	nameAndExclude := model.NewNameAndExclude("dataset.events", nil, []string{"payload"})

	tests := []struct {
		name            string
		columnName      string
		oid             uint32
		wantPassthrough bool
		wantErr         string
	}{
		{
			name:            "allows json",
			columnName:      "payload",
			oid:             pgtype.JSONOID,
			wantPassthrough: true,
		},
		{
			name:            "allows jsonb",
			columnName:      "payload",
			oid:             pgtype.JSONBOID,
			wantPassthrough: true,
		},
		{
			name:       "rejects json array",
			columnName: "payload",
			oid:        pgtype.JSONArrayOID,
			wantErr:    "unsupported Postgres JSON array type OID",
		},
		{
			name:       "rejects non JSON",
			columnName: "payload",
			oid:        pgtype.Int4OID,
			wantErr:    "non-JSON Postgres type OID",
		},
		{
			name:       "ignores columns outside allowlist",
			columnName: "other_payload",
			oid:        pgtype.Int4OID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newTestCDCSource(t)
			p.destinationType = protos.DBType_BIGQUERY
			p.tableNameSchemaMapping = map[string]*protos.TableSchema{
				"dataset.events": cdcTestTableSchema("payload", types.QValueKindJSON),
			}

			passthrough, err := p.shouldPassthroughJSONColumn(nameAndExclude, &pglogrepl.RelationMessageColumn{
				Name:     tt.columnName,
				DataType: tt.oid,
			})
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.wantPassthrough, passthrough)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
