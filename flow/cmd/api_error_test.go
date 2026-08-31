package cmd

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func TestTableErrorDetailsLimitViolationsAndIncludeTotalCount(t *testing.T) {
	tables := make([]common.QualifiedTable, common.MaxTablesInErrorDetails+2)
	for i := range tables {
		tables[i] = common.QualifiedTable{Namespace: "public", Table: fmt.Sprintf("table_%02d", i)}
	}

	for _, details := range [][]protoadapt.MessageV1{
		NewSourceTableMissingErrorDetails(tables),
		NewTablesNotInPublicationErrorDetails("publication", tables),
	} {
		require.Len(t, details, 2)
		st, err := status.New(codes.FailedPrecondition, "table validation failed").WithDetails(details...)
		require.NoError(t, err)
		decodedDetails := st.Details()

		info, ok := decodedDetails[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		require.Equal(t, strconv.Itoa(len(tables)), info.Metadata[common.ErrorMetadataTableCount])

		failure, ok := decodedDetails[1].(*errdetails.PreconditionFailure)
		require.True(t, ok)
		require.Len(t, failure.Violations, common.MaxTablesInErrorDetails)
		require.Equal(t, "public.table_00", failure.Violations[0].Subject)
		require.Equal(t, "public.table_19", failure.Violations[common.MaxTablesInErrorDetails-1].Subject)
	}
}
