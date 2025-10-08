package connbigquery

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials/downscope"
)

func (c *BigQueryConnector) storageDownScopedToken(ctx context.Context, bucketName string, prefix string) (*auth.Token, error) {
	accessBoundary := []downscope.AccessBoundaryRule{
		{
			AvailableResource:    "//storage.googleapis.com/projects/_/buckets/" + bucketName,
			AvailablePermissions: []string{"inRole:roles/storage.objectViewer"},
			Condition: &downscope.AvailabilityCondition{
				Expression: fmt.Sprintf("resource.name.startsWith('projects/_/buckets/%s/objects/%s')", bucketName, prefix),
			},
		},
	}

	tp, err := downscope.NewCredentials(&downscope.Options{Credentials: c.credentials, Rules: accessBoundary})
	if err != nil {
		return nil, fmt.Errorf("failed to generate downscoped token provider: %w", err)
	}

	token, err := tp.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to generate downscoped token: %w", err)
	}
	return token, nil
}

type gcsPath struct {
	*url.URL
}

func parseGCSPath(gcsPathStr string) (*gcsPath, error) {
	u, err := url.Parse(gcsPathStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "gs" {
		return nil, &url.Error{Op: "parse", URL: gcsPathStr, Err: fmt.Errorf("invalid scheme: %s, expected gs", u.Scheme)}
	}

	return &gcsPath{URL: u}, nil
}

func (p *gcsPath) Bucket() string {
	return p.Host
}

// QueryPrefix returns the path with a trailing slash.
// It is used as the prefix to query objects in the bucket.
// For example, if the path is "folder/subfolder", it returns "folder/subfolder/".
// If the path is "folder/subfolder/", it also returns "folder/subfolder/".
func (p *gcsPath) QueryPrefix() string {
	return strings.TrimPrefix(p.Path, "/") + "/"
}

func (p *gcsPath) JoinPath(elem ...string) *gcsPath {
	return &gcsPath{URL: p.URL.JoinPath(elem...)}
}
