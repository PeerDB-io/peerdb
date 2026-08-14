package connbigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestTableHasPrimaryKey(t *testing.T) {
	tests := []struct {
		name     string
		metadata *bigquery.TableMetadata
		want     bool
	}{
		{
			name:     "no constraints at all",
			metadata: &bigquery.TableMetadata{},
			want:     false,
		},
		{
			name:     "constraints present but no primary key",
			metadata: &bigquery.TableMetadata{TableConstraints: &bigquery.TableConstraints{}},
			want:     false,
		},
		{
			name: "primary key present but empty columns",
			metadata: &bigquery.TableMetadata{
				TableConstraints: &bigquery.TableConstraints{PrimaryKey: &bigquery.PrimaryKey{Columns: []string{}}},
			},
			want: false,
		},
		{
			name: "real primary key",
			metadata: &bigquery.TableMetadata{
				TableConstraints: &bigquery.TableConstraints{PrimaryKey: &bigquery.PrimaryKey{Columns: []string{"id"}}},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tableHasPrimaryKey(tt.metadata))
		})
	}
}

func TestRejectKeylessReplacingMergeTree(t *testing.T) {
	tests := []struct {
		name   string
		hasPK  bool
		engine protos.TableEngine
		want   bool
	}{
		{
			name:   "no PK, default engine (ReplacingMergeTree) - reject",
			hasPK:  false,
			engine: protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE,
			want:   true,
		},
		{
			name:   "no PK, explicit MergeTree - allowed",
			hasPK:  false,
			engine: protos.TableEngine_CH_ENGINE_MERGE_TREE,
			want:   false,
		},
		{
			name:   "has PK, ReplacingMergeTree - allowed",
			hasPK:  true,
			engine: protos.TableEngine_CH_ENGINE_REPLACING_MERGE_TREE,
			want:   false,
		},
		{
			name:   "has PK, MergeTree - allowed",
			hasPK:  true,
			engine: protos.TableEngine_CH_ENGINE_MERGE_TREE,
			want:   false,
		},
		{
			name:   "no PK, replicated ReplacingMergeTree - reject (same collapsing dedup engine, just replicated)",
			hasPK:  false,
			engine: protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE,
			want:   true,
		},
		{
			name:   "has PK, replicated ReplacingMergeTree - allowed",
			hasPK:  true,
			engine: protos.TableEngine_CH_ENGINE_REPLICATED_REPLACING_MERGE_TREE,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, rejectKeylessReplacingMergeTree(tt.hasPK, tt.engine))
		})
	}
}
