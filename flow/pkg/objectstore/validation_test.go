package objectstore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// S3 Tests:

func newFakeS3Client(t *testing.T, server *httptest.Server) *s3.Client {
	t.Helper()
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:   server.Client(),
	})
}

func TestNewS3StagingValidator_HappyPath(t *testing.T) {
	var puts, deletes atomic.Int32
	var putPath, deletePath atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			puts.Add(1)
			putPath.Store(r.URL.Path)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			deletes.Add(1)
			deletePath.Store(r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s on %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := newFakeS3Client(t, server)
	err := NewS3StagingValidator(client, "my-bucket", "stage/1")(t.Context())
	require.NoError(t, err)
	require.Equal(t, int32(1), puts.Load(), "expected exactly one PUT")
	require.Equal(t, int32(1), deletes.Load(), "expected exactly one DELETE")

	put, _ := putPath.Load().(string)
	del, _ := deletePath.Load().(string)
	require.Equal(t, put, del, "PUT and DELETE must target the same key")
	require.True(t,
		strings.HasPrefix(put, "/my-bucket/stage/1/"+stagingCheckObjectPrefix),
		"unexpected key path %q", put,
	)
}

func TestNewS3StagingValidator_PutFailure(t *testing.T) {
	var puts, deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		deletes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newFakeS3Client(t, server)
	err := NewS3StagingValidator(client, "b", "p")(t.Context())
	require.ErrorContains(t, err, "failed to write to bucket")
	require.Positive(t, puts.Load(), "PUT must have been attempted")
	require.Equal(t, int32(0), deletes.Load(), "DELETE must not run when PUT fails")
}

func TestNewS3StagingValidator_DeleteFailure(t *testing.T) {
	var deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			deletes.Add(1)
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()

	client := newFakeS3Client(t, server)
	err := NewS3StagingValidator(client, "b", "p")(t.Context())
	require.ErrorContains(t, err, "failed to delete from bucket")
	require.Positive(t, deletes.Load(), "DELETE must have been attempted")
}

func TestNewS3StagingValidator_EmptyPrefix(t *testing.T) {
	var seenPath atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath.Store(r.URL.Path)
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	client := newFakeS3Client(t, server)
	require.NoError(t, NewS3StagingValidator(client, "bkt", "")(t.Context()))

	path, _ := seenPath.Load().(string)
	require.True(t, strings.HasPrefix(path, "/bkt/"+stagingCheckObjectPrefix), "got %q", path)
	require.NotContains(t, path, "//"+stagingCheckObjectPrefix, "must not produce double slash")
}

// GCS tests:

func newFakeGCSClient(t *testing.T, server *httptest.Server) *storage.Client {
	t.Helper()
	// STORAGE_EMULATOR_HOST disables auth in the storage SDK and routes the
	// JSON API to the test server.
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL, "http://"))

	client, err := storage.NewClient(t.Context(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL),
		option.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestNewGCSStagingValidator_HappyPath(t *testing.T) {
	var uploads, deletes atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("GCS request: %s %s", r.Method, r.URL.Path)
		switch {
		case r.Method == http.MethodPost && (strings.Contains(r.URL.Path, "/b/my-bucket/o")):
			uploads.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"obj","bucket":"my-bucket"}`))

		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/b/my-bucket/o/"):
			deletes.Add(1)
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Errorf("unexpected GCS request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := newFakeGCSClient(t, server)
	err := NewGCSStagingValidator(client, "my-bucket", "stage/1")(t.Context())
	require.NoError(t, err)
	require.Equal(t, int32(1), uploads.Load(), "expected exactly one upload")
	require.Equal(t, int32(1), deletes.Load(), "expected exactly one delete")
}

func TestNewGCSStagingValidator_UploadFailure(t *testing.T) {
	var deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/b/"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"forbidden"}}`))
		case r.Method == http.MethodDelete:
			deletes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected GCS request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := newFakeGCSClient(t, server)
	err := NewGCSStagingValidator(client, "b", "p")(t.Context())
	require.ErrorContains(t, err, "failed to finalize test object in GCS")
	require.Equal(t, int32(0), deletes.Load(), "DELETE must not run when upload fails")
}

func TestNewGCSStagingValidator_DeleteFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/b/"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"obj","bucket":"b"}`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"forbidden"}}`))
		default:
			t.Errorf("unexpected GCS request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := newFakeGCSClient(t, server)
	err := NewGCSStagingValidator(client, "b", "p")(t.Context())
	require.ErrorContains(t, err, "failed to delete test object from GCS")
}
