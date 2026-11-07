//go:build tilt

package bigquery_clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
)

// bqFaultSource is a BigQuery query-CDC source. Table names are unique per
// suite, because the dataset is shared.
type bqFaultSource struct {
	t      *testing.T
	source *bigQuerySource
	fqns   map[string]string
	mu     sync.Mutex
}

func (b *bqFaultSource) fqn(table string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fqns[table]
}

func (b *bqFaultSource) CreateTable(ctx context.Context, table string) error {
	fqn := createBigQueryCdcSourceTable(ctx, b.t, b.source, table, false)
	b.mu.Lock()
	b.fqns[table] = fqn
	b.mu.Unlock()
	return nil
}

func (b *bqFaultSource) Insert(ctx context.Context, table string, row e2e.FaultRow, _ bool) error {
	bqInsertRows(ctx, b.t, b.source, b.fqn(table), []bqCdcRow{{ID: row.ID, Val: row.Val}})
	return nil
}

func (b *bqFaultSource) InsertMany(ctx context.Context, table string, rows []e2e.FaultRow, _ bool) error {
	bqRows := make([]bqCdcRow, len(rows))
	for i, row := range rows {
		bqRows[i] = bqCdcRow{ID: row.ID, Val: row.Val}
	}
	bqInsertRows(ctx, b.t, b.source, b.fqn(table), bqRows)
	return nil
}

func (b *bqFaultSource) AddExtraColumn(context.Context, string) error {
	return errors.ErrUnsupported
}

func (b *bqFaultSource) Durable(context.Context, string) (map[string]int64, error) {
	return nil, nil
}

func (s BigQueryClickhouseSuite) faultWorkload() e2e.FaultWorkload {
	t := s.T()
	t.Helper()
	ctx := t.Context()
	source := s.Source().(*bigQuerySource)
	dest, err := e2e.NewChFaultDestination(ctx, s.Peer())
	require.NoError(t, err)
	catalog, err := e2e.CatalogTestAccessPool()
	require.NoError(t, err)
	return e2e.FaultWorkload{
		Suite:    s,
		Source:   &bqFaultSource{t: t, source: source, fqns: map[string]string{}},
		Dest:     dest,
		Progress: e2e.QueryCDCProgress(catalog),
		Tables: func(id string) []e2e.FaultTable {
			var tables []e2e.FaultTable
			for _, suffix := range []string{"a", "b"} {
				name := e2e.AddSuffix(s, fmt.Sprintf("fault_%s_%s", id, suffix))
				identifier := fmt.Sprintf("%s.%s", source.config.DatasetId, name)
				tables = append(tables, e2e.FaultTable{
					Source:      name,
					Dest:        name + "_dst",
					CatalogName: s.DestinationTable(name + "_dst"),
					ProgressKey: identifier,
					HookTable:   identifier,
				})
			}
			return tables
		},
		FlowConfig: func(flowName string, tables []e2e.FaultTable) *protos.FlowConnectionConfigs {
			var mappings []*protos.TableMapping
			for _, table := range tables {
				mappings = append(mappings, &protos.TableMapping{
					SourceTableIdentifier:      table.ProgressKey,
					DestinationTableIdentifier: s.DestinationTable(table.Dest),
					BigqueryCdcEventsFunction:  protos.BigqueryCdcEventsFunction_BIGQUERY_CDC_EVENTS_FUNCTION_APPENDS,
				})
			}
			gen := e2e.FlowConnectionGenerationConfig{
				FlowJobName:   flowName,
				TableMappings: mappings,
				Destination:   s.Peer().Name,
			}
			config := gen.GenerateFlowConnectionConfigs(s)
			config.DoInitialSnapshot = true
			config.InitialSnapshotOnly = false
			config.SnapshotStagingPath = bigQueryTestStagingPath(s, tables[0].Source)
			config.SourceConnectorConfig = &protos.FlowConnectionConfigs_BigqueryCdcConfig{
				BigqueryCdcConfig: &protos.BigqueryCdcConfig{
					ReplicationMethod: protos.BigQueryReplicationMethod_BIGQUERY_REPLICATION_METHOD_EVENTS,
				},
			}
			config.IdleTimeoutSeconds = 5
			config.Env = map[string]string{
				"PEERDB_QUERY_CDC_SAFETY_LAG_SECONDS": "5",
				// InsertLarge rows exceed this, so their batch spans several files
				// and normalize checkpoints between them.
				"PEERDB_S3_BYTES_PER_AVRO_FILE": "4096",
			}
			return config
		},
		Kind:              testhooks.WorkloadBQToCH,
		BufferLimit:       e2e.FaultBuffer,
		IndependentTables: true,
	}
}

func (s BigQueryClickhouseSuite) Test_FaultKnown_NormalizeMissesTheFinalRequestAndLeavesDataPending() {
	e2e.RunKnownFailure(s.faultWorkload(), "q_missed_req")
}

func (s BigQueryClickhouseSuite) Test_FaultKnown_QueryStartupReplacesANewerRequestWithAnOlderOne() {
	e2e.RunKnownFailure(s.faultWorkload(), "q_startup")
}

func TestFaultShard(t *testing.T) {
	e2e.RunFaultShards(t, testhooks.WorkloadBQToCH, SetupBigQueryClickhouseSuite, BigQueryClickhouseSuite.faultWorkload)
}
