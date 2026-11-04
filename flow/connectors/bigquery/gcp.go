package connbigquery

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials/downscope"
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// downscope returns token exchange failures as an untyped string.
var downscopeExchangeErrorRe = regexp.MustCompile(`(?s)^downscope: unable to exchange token, (\d{3}): (.*)$`)

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
		return nil, exceptions.NewGCSError(fmt.Errorf("failed to generate downscoped token: %w", wrapDownscopeError(err)))
	}
	return token, nil
}

// wrapDownscopeError extracts the HTTP status into a googleapi.Error for classification.
func wrapDownscopeError(err error) error {
	m := downscopeExchangeErrorRe.FindStringSubmatch(err.Error())
	if m == nil {
		return err
	}
	code, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		return err
	}
	apiErr := &googleapi.Error{Code: code, Body: m[2]}
	apiErr.Wrap(err)
	return apiErr
}

func (c *BigQueryConnector) isApiErrorWithStatusCode(err error, code int) bool {
	if apiErr, ok := errors.AsType[*googleapi.Error](err); ok && apiErr.Code == code {
		return true
	}
	return false
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

	if u.Host == "" {
		return nil, &url.Error{Op: "parse", URL: gcsPathStr, Err: fmt.Errorf("bucket name is required")}
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
// If the path is empty or just "/", it returns an empty string.
func (p *gcsPath) QueryPrefix() string {
	path := strings.TrimPrefix(p.Path, "/")
	if path == "" {
		return ""
	}
	return path + "/"
}

func (p *gcsPath) JoinPath(elem ...string) *gcsPath {
	return &gcsPath{URL: p.URL.JoinPath(elem...)}
}
