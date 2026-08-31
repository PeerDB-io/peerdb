package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableErrorsLimitDisplayedTables(t *testing.T) {
	tables := make([]QualifiedTable, MaxTablesInErrorDetails+2)
	for i := range tables {
		tables[i] = QualifiedTable{Namespace: "public", Table: fmt.Sprintf("table_%02d", i)}
	}

	for _, err := range []error{
		NewSourceTablesMissingError(tables),
		NewTablesNotInPublicationError("publication", tables),
	} {
		message := err.Error()
		require.Contains(t, message, "public.table_19")
		require.NotContains(t, message, "public.table_20")
		require.Contains(t, message, "and 2 more")
		require.Equal(t, MaxTablesInErrorDetails, strings.Count(message, "public.table_"))
	}
}
