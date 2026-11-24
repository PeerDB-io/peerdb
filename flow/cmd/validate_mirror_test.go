package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func tableMappingsForPayloadTest(n int) []*protos.TableMapping {
	mappings := make([]*protos.TableMapping, 0, n)
	for i := range n {
		mappings = append(mappings, &protos.TableMapping{
			SourceTableIdentifier:      fmt.Sprintf("public.source_table_%d", i),
			DestinationTableIdentifier: fmt.Sprintf("destination_table_%d", i),
			Columns: []*protos.ColumnSetting{
				{SourceName: "id", DestinationName: "id", DestinationType: "Int64"},
				{SourceName: "payload", DestinationName: "payload", DestinationType: "String"},
			},
		})
	}
	return mappings
}

func TestCheckCDCFlowPayloadSize(t *testing.T) {
	t.Parallel()

	small := &protos.FlowConnectionConfigsCore{FlowJobName: "mirror", TableMappings: tableMappingsForPayloadTest(10)}
	require.Nil(t, checkCDCFlowPayloadSize(small))

	large := &protos.FlowConnectionConfigsCore{FlowJobName: "mirror", TableMappings: tableMappingsForPayloadTest(10000)}
	apiErr := checkCDCFlowPayloadSize(large)
	require.NotNil(t, apiErr)
	require.Contains(t, apiErr.Error(), "split it into smaller mirrors")
}
