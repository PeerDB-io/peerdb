package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

type grantTestResult struct {
	granted bool
	err     error
}

type grantTestRow struct {
	result grantTestResult
}

func (r grantTestRow) Err() error {
	return r.result.err
}

func (r grantTestRow) Scan(dest ...any) error {
	if r.result.err != nil {
		return r.result.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("expected one scan destination, got %d", len(dest))
	}
	granted, ok := dest[0].(*bool)
	if !ok {
		return fmt.Errorf("expected *bool scan destination, got %T", dest[0])
	}
	*granted = r.result.granted
	return nil
}

func (r grantTestRow) ScanStruct(any) error {
	return errors.New("ScanStruct is not supported")
}

type grantTestConn struct {
	driver.Conn
	results map[string]grantTestResult
	queries []string
}

func (c *grantTestConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.queries = append(c.queries, query)
	result, ok := c.results[query]
	if !ok {
		result.err = fmt.Errorf("unexpected query %q", query)
	}
	return grantTestRow{result: result}
}

func grantSyntaxError() error {
	return &clickhousego.Exception{
		Code:    int32(chproto.ErrSyntaxError),
		Message: "syntax error",
	}
}

func TestValidateStagingAccessGrant(t *testing.T) {
	tests := []struct {
		name         string
		accessMethod string
		results      map[string]grantTestResult
		wantQueries  []string
		wantErr      string
	}{
		{
			name:         "new S3 grant",
			accessMethod: "S3",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON S3": {granted: true},
			},
			wantQueries: []string{"CHECK GRANT READ ON S3"},
		},
		{
			name:         "new URL grant",
			accessMethod: "URL",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON URL": {granted: true},
			},
			wantQueries: []string{"CHECK GRANT READ ON URL"},
		},
		{
			name:         "legacy S3 grant",
			accessMethod: "S3",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON S3": {err: grantSyntaxError()},
				"CHECK GRANT S3 ON *.*":  {granted: true},
			},
			wantQueries: []string{"CHECK GRANT READ ON S3", "CHECK GRANT S3 ON *.*"},
		},
		{
			name:         "missing new grant",
			accessMethod: "URL",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON URL": {granted: false},
			},
			wantQueries: []string{"CHECK GRANT READ ON URL"},
			wantErr:     "user lacks READ on URL",
		},
		{
			name:         "missing legacy grant",
			accessMethod: "URL",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON URL": {err: grantSyntaxError()},
				"CHECK GRANT URL ON *.*":  {granted: false},
			},
			wantQueries: []string{"CHECK GRANT READ ON URL", "CHECK GRANT URL ON *.*"},
			wantErr:     "user lacks READ on URL",
		},
		{
			name:         "CHECK GRANT unsupported",
			accessMethod: "S3",
			results: map[string]grantTestResult{
				"CHECK GRANT READ ON S3": {err: grantSyntaxError()},
				"CHECK GRANT S3 ON *.*":  {err: grantSyntaxError()},
			},
			wantQueries: []string{"CHECK GRANT READ ON S3", "CHECK GRANT S3 ON *.*"},
		},
		{
			name:         "unsupported access method",
			accessMethod: "AZURE",
			wantErr:      `unsupported ClickHouse staging access method "AZURE"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &grantTestConn{results: tt.results}
			err := validateStagingAccessGrant(t.Context(), nopLogger{}, conn, tt.accessMethod)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantQueries, conn.queries)
		})
	}
}
