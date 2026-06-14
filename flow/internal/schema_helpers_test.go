package internal

import (
	"log/slog"
	"testing"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func mapping(srcNs, srcTable, dstNs, dstTable string, exclude ...string) *protos.TableMapping {
	return &protos.TableMapping{
		SourceTable:      &protos.QualifiedTable{Namespace: srcNs, Table: srcTable},
		DestinationTable: &protos.QualifiedTable{Namespace: dstNs, Table: dstTable},
		Exclude:          exclude,
	}
}

func TestAdditionalTablesHasOverlap(t *testing.T) {
	current := []*protos.TableMapping{
		mapping("sch.ema", "ta.ble", "", "dst.table"),
		mapping("public", "plain", "public", "plain_dst"),
	}

	for _, tc := range []struct {
		name             string
		additional       []*protos.TableMapping
		checkDestination bool
		want             bool
	}{
		{
			name:       "dotted source overlap is struct-exact",
			additional: []*protos.TableMapping{mapping("sch.ema", "ta.ble", "", "other_dst")},
			want:       true,
		},
		{
			// {"sch", "ema.ta.ble"} renders the same dotted string as {"sch.ema", "ta.ble"}
			// but is a different table; struct comparison must NOT flag it
			name:       "dotted-string collision is not struct overlap",
			additional: []*protos.TableMapping{mapping("sch", "ema.ta.ble", "", "other_dst")},
			want:       false,
		},
		{
			name:             "dotted destination overlap",
			additional:       []*protos.TableMapping{mapping("public", "fresh", "", "dst.table")},
			checkDestination: true,
			want:             true,
		},
		{
			name:             "destination overlap ignored without checkDestination",
			additional:       []*protos.TableMapping{mapping("public", "fresh", "", "dst.table")},
			checkDestination: false,
			want:             false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AdditionalTablesHasOverlap(current, tc.additional, tc.checkDestination); got != tc.want {
				t.Errorf("AdditionalTablesHasOverlap = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildProcessedSchemaMappingDottedKeysAndExclusion(t *testing.T) {
	src := common.QualifiedTable{Namespace: "sch.ema", Table: "ta.ble"}
	dst := common.QualifiedTable{Table: "dst.table"}
	schema := &protos.TableSchema{
		Columns: []*protos.FieldDescription{
			{Name: "id"},
			{Name: "secret"},
			{Name: "val"},
		},
		PrimaryKeyColumns: []string{"id"},
	}

	processed := BuildProcessedSchemaMapping(
		[]*protos.TableMapping{mapping(src.Namespace, src.Table, dst.Namespace, dst.Table, "secret")},
		map[common.QualifiedTable]*protos.TableSchema{src: schema},
		log.NewStructuredLogger(slog.Default()),
	)

	got, ok := processed[dst]
	if !ok {
		t.Fatalf("processed mapping not keyed by destination struct, keys: %v", processed)
	}
	if len(got.Columns) != 2 || got.Columns[0].Name != "id" || got.Columns[1].Name != "val" {
		t.Errorf("exclusion not applied, columns: %v", got.Columns)
	}
	if len(got.PrimaryKeyColumns) != 1 || got.PrimaryKeyColumns[0] != "id" {
		t.Errorf("primary keys wrong: %v", got.PrimaryKeyColumns)
	}
}
