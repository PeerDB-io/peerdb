package connelasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type searchInfoResponse struct {
	Version struct {
		Distribution string `json:"distribution"`
	} `json:"version"`
	Tagline string `json:"tagline"`
}

func detectSearchBackend(ctx context.Context, addresses []string, transport http.RoundTripper) (searchBackend, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("search peer has no configured addresses")
	}

	infoURL := strings.TrimRight(addresses[0], "/") + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("failed to create backend probe request: %w", err)
	}

	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to probe search backend: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read backend probe response: %w", err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("failed to probe search backend from %s: %s: %s",
			infoURL, resp.Status, strings.TrimSpace(string(payload)))
	}

	if strings.EqualFold(resp.Header.Get("X-Elastic-Product"), "Elasticsearch") {
		return searchBackendElastic, nil
	}

	var info searchInfoResponse
	if err := json.Unmarshal(payload, &info); err == nil {
		if strings.EqualFold(info.Version.Distribution, "opensearch") ||
			strings.Contains(strings.ToLower(info.Tagline), "opensearch") {
			return searchBackendOpenSearch, nil
		}
	}

	return searchBackendElastic, nil
}
