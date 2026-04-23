package connsnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/log"
	"golang.org/x/time/rate"
)

const (
	// DefaultMaxChunkBytes is the maximum size in bytes for a single Snowpipe Streaming
	// NDJSON chunk. 4MB is the Snowflake hard limit; chunks are split at this boundary.
	DefaultMaxChunkBytes = 4 * 1024 * 1024
	// DefaultRateLimitPerSecond is the default maximum number of Snowpipe Streaming
	// append-rows API calls per second. Matches the typical per-account quota.
	DefaultRateLimitPerSecond = 10
	// DefaultIngestTimeoutSeconds is the default number of seconds to wait for Snowflake
	// to confirm row ingestion per flush via GetChannelStatus polling.
	DefaultIngestTimeoutSeconds = 120
	tokenRefreshBuffer          = 1 * time.Minute

	// sendChunk retry limits
	maxChunkSendAttempts = 4               // initial + 3 retries
	initialBackoff       = 2 * time.Second // doubles on each transient retry, capped at 30s
	maxBackoff           = 30 * time.Second
)

// channelReopenErrorCodes are Snowpipe Streaming error codes that indicate a channel
// must be reopened before retrying.
var channelReopenErrorCodes = map[string]bool{
	"ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER":           true,
	"ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER":         true,
	"ERR_CHANNEL_MUST_BE_REOPENED":                     true,
	"ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP": true,
	"ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED": true,
	"STALE_PIPE_CACHE":                                 true,
}

// StreamingErrorResponse represents an error response from the Snowpipe Streaming API.
type StreamingErrorResponse struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	ResultHandler string `json:"result_handler,omitempty"`
	// HTTPStatus is populated from the HTTP response status code, not from the JSON body.
	HTTPStatus int `json:"-"`
}

// ChannelStatus contains per-channel ingestion metrics returned by the Snowpipe Streaming
// bulk channel status endpoint. It includes error details that SYSTEM$PIPE_STATUS does not.
type ChannelStatus struct {
	ChannelStatusCode               string `json:"channel_status_code"`
	LastCommittedOffsetToken        string `json:"last_committed_offset_token"`
	RowsInserted                    int    `json:"rows_inserted"`
	RowsParsed                      int    `json:"rows_parsed"`
	RowsErrorCount                  int    `json:"rows_error_count"`
	LastErrorMessage                string `json:"last_error_message"`
	SnowflakeAvgProcessingLatencyMs int    `json:"snowflake_avg_processing_latency_ms"`
}

// Error implements the error interface.
func (e *StreamingErrorResponse) Error() string {
	return fmt.Sprintf("snowpipe streaming error %s (http %d): %s", e.Code, e.HTTPStatus, e.Message)
}

// IsChannelReopenError returns true if the error code indicates the channel should be reopened.
func (e *StreamingErrorResponse) IsChannelReopenError() bool {
	return channelReopenErrorCodes[e.Code]
}

// IsAuthError returns true if the error indicates an expired or invalid token.
// Snowflake may report HTTP 401 with an empty code or a specific auth error code.
func (e *StreamingErrorResponse) IsAuthError() bool {
	return e.HTTPStatus == http.StatusUnauthorized
}

// IsTransientError returns true for errors that are worth retrying with backoff:
// HTTP 429 (rate limited by Snowflake account quotas) and 5xx server errors.
// Client errors other than 429 (e.g. 400 schema mismatch) are permanent and should
// not be retried.
func (e *StreamingErrorResponse) IsTransientError() bool {
	return e.HTTPStatus == http.StatusTooManyRequests ||
		(e.HTTPStatus >= 500 && e.HTTPStatus < 600)
}

// StreamingChannel represents a single Snowpipe Streaming channel with its state.
type StreamingChannel struct {
	// mu protects continuationToken and rowsInserted; both are reset on invalidation.
	mu                sync.Mutex
	continuationToken string
	// rowsInserted is the cumulative row count confirmed by the last GetChannelStatus call.
	// It is used as the baseline for delivery confirmation: if the server reports a higher
	// count after a flush, the batch has been committed. Reset to 0 on channel reopen.
	rowsInserted int
	// rateLimiter enforces the per-channel request rate against the Snowpipe Streaming API.
	rateLimiter *rate.Limiter
}

func (ch *StreamingChannel) getContinuationToken() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.continuationToken
}

func (ch *StreamingChannel) setContinuationToken(token string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.continuationToken = token
}

func (ch *StreamingChannel) getRowsInserted() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.rowsInserted
}

func (ch *StreamingChannel) setRowsInserted(n int) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.rowsInserted = n
}

func (ch *StreamingChannel) invalidate() {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.continuationToken = ""
	ch.rowsInserted = 0 // Snowflake resets per-channel counters on reopen
}

// StreamingChannelManager manages Snowpipe Streaming auth state and channel lifecycle.
type StreamingChannelManager struct {
	// sfConfig holds the Snowflake connection parameters (account, user, private key).
	sfConfig *gosnowflake.Config
	// channels is the live map of channel name → StreamingChannel, keyed by channel name.
	channels   map[string]*StreamingChannel
	channelsMu sync.Mutex

	// Auth state: scoped OAuth token, ingest hostname, and expiry time. Protected by refreshMu.
	scopedToken string
	ingestHost  string
	tokenExpiry time.Time
	refreshMu   sync.Mutex

	// httpClient is used for all Snowpipe Streaming REST calls. It has a 30-second
	// timeout and a reusable transport; using a shared client avoids repeated
	// TCP handshakes and prevents indefinite hangs on unresponsive Snowflake endpoints.
	httpClient *http.Client
	// logger is the Temporal activity logger propagated from the connector.
	logger log.Logger

	// maxChunkBytes is the maximum NDJSON payload size per append-rows request.
	maxChunkBytes int
	// rateLimitPerSecond caps the number of append-rows API calls per second per channel.
	rateLimitPerSecond int
	// ingestTimeoutSeconds is the maximum time to wait per flush for Snowflake to confirm
	// row ingestion. Configurable via streaming_ingest_timeout_seconds on the peer.
	ingestTimeoutSeconds int

	// metrics holds OpenTelemetry instruments for the streaming path.
	metrics *streamingMetrics
}

// NewStreamingChannelManager creates a new channel manager for Snowpipe Streaming.
// Zero or negative values for rateLimitPerSec, maxChunkBytes, and ingestTimeoutSeconds
// fall back to DefaultRateLimitPerSecond, DefaultMaxChunkBytes, and DefaultIngestTimeoutSeconds.
func NewStreamingChannelManager(
	sfConfig *gosnowflake.Config,
	logger log.Logger,
	rateLimitPerSec int,
	maxChunkBytes int,
	ingestTimeoutSeconds int,
) *StreamingChannelManager {
	if rateLimitPerSec <= 0 {
		rateLimitPerSec = DefaultRateLimitPerSecond
	}
	if maxChunkBytes <= 0 {
		maxChunkBytes = DefaultMaxChunkBytes
	}
	if ingestTimeoutSeconds <= 0 {
		ingestTimeoutSeconds = DefaultIngestTimeoutSeconds
	}

	return &StreamingChannelManager{
		sfConfig: sfConfig,
		channels: make(map[string]*StreamingChannel),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger:               logger,
		maxChunkBytes:        maxChunkBytes,
		rateLimitPerSecond:   rateLimitPerSec,
		ingestTimeoutSeconds: ingestTimeoutSeconds,
		metrics:              newStreamingMetrics(),
	}
}

// refreshAuth refreshes the JWT → ingest host → scoped token chain when the cached
// token is within one minute of expiry. Always acquires the mutex before reading
// token state to avoid a data race with Close() which writes scopedToken = "".
func (m *StreamingChannelManager) refreshAuth(ctx context.Context) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	// Check inside the lock — no unlocked fast-path to avoid a data race (refreshMu
	// is acquired once per sync batch, so the overhead is negligible).
	if time.Now().Before(m.tokenExpiry.Add(-tokenRefreshBuffer)) && m.scopedToken != "" {
		return nil
	}

	jwtToken, err := PrepareJWTToken(m.sfConfig)
	if err != nil {
		return fmt.Errorf("failed to prepare JWT token: %w", err)
	}

	account := m.sfConfig.Account
	ingestHost, err := GetIngestHost(ctx, jwtToken, account, m.httpClient)
	if err != nil {
		return fmt.Errorf("failed to get ingest host: %w", err)
	}

	scopedToken, expiresAt, err := GetScopedToken(ctx, jwtToken, account, ingestHost, m.httpClient)
	if err != nil {
		return fmt.Errorf("failed to get scoped token: %w", err)
	}

	m.scopedToken = scopedToken
	m.ingestHost = ingestHost
	m.tokenExpiry = expiresAt
	return nil
}

// openChannelResponse is the response from opening a Snowpipe Streaming channel.
type openChannelResponse struct {
	NextContinuationToken string        `json:"next_continuation_token"`
	ChannelStatus         ChannelStatus `json:"channel_status"`
}

// getOrCreateChannel returns an existing channel or opens a new one.
func (m *StreamingChannelManager) getOrCreateChannel(
	ctx context.Context,
	db, schema, pipe, channelName string,
	forceReopen bool,
) (*StreamingChannel, error) {
	m.channelsMu.Lock()
	if !forceReopen {
		if ch, ok := m.channels[channelName]; ok {
			m.channelsMu.Unlock()
			return ch, nil
		}
	}
	m.channelsMu.Unlock()

	// Open channel via REST API
	reqURL := fmt.Sprintf("https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s",
		m.ingestHost, db, schema, pipe, channelName)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, fmt.Errorf("failed to create open channel request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read open channel response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("open channel failed with status %d: %s", resp.StatusCode, string(body))
	}

	var openResp openChannelResponse
	if err := json.Unmarshal(body, &openResp); err != nil {
		return nil, fmt.Errorf("failed to parse open channel response: %w", err)
	}

	ch := &StreamingChannel{
		continuationToken: openResp.NextContinuationToken,
		// Seed rowsInserted from the open-channel response so the first delivery check
		// has a correct baseline (Snowflake returns cumulative counters in the response).
		rowsInserted: openResp.ChannelStatus.RowsInserted,
		rateLimiter:  rate.NewLimiter(rate.Limit(m.rateLimitPerSecond), 1),
	}

	m.channelsMu.Lock()
	m.channels[channelName] = ch
	m.channelsMu.Unlock()

	return ch, nil
}

// appendRowsResponse is the response from appending rows to a channel.
type appendRowsResponse struct {
	NextContinuationToken string `json:"next_continuation_token"`
}

// appendRows sends NDJSON encoded rows to the channel.
// offsetToken, when non-empty, is sent as the ?offsetToken= query parameter.
// Snowflake persists it durably: a second Append Rows call with the same offsetToken
// on the same channel is a no-op, closing the at-least-once window on Temporal retries.
func (m *StreamingChannelManager) appendRows(
	ctx context.Context,
	db, schema, pipe, channelName string,
	ch *StreamingChannel,
	encodedRows []byte,
	offsetToken string,
) (*StreamingErrorResponse, error) {
	contToken := ch.getContinuationToken()
	reqURL := fmt.Sprintf("https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows?continuationToken=%s",
		m.ingestHost, db, schema, pipe, channelName, contToken)
	if offsetToken != "" {
		reqURL += "&offsetToken=" + offsetToken
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(encodedRows))
	if err != nil {
		return nil, fmt.Errorf("failed to create append rows request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to append rows: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read append rows response: %w", err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var appendResp appendRowsResponse
		if err := json.Unmarshal(body, &appendResp); err != nil {
			return nil, fmt.Errorf("failed to parse append rows response: %w", err)
		}
		ch.setContinuationToken(appendResp.NextContinuationToken)
		return nil, nil
	}

	// Parse structured error response. Populate HTTPStatus so callers can classify the error
	// without re-examining the HTTP layer.
	var errResp StreamingErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		return nil, fmt.Errorf("append rows failed with status %d: %s", resp.StatusCode, string(body))
	}
	errResp.HTTPStatus = resp.StatusCode
	return &errResp, nil
}

// sendChunkWithRetry sends a chunk of NDJSON rows to the Snowpipe Streaming channel.
//
// Retry behaviour by error type:
//   - Channel-reopen errors: channel is invalidated and reopened, then retried once.
//   - Auth errors (HTTP 401): token is refreshed via refreshAuth, then retried.
//   - Transient errors (HTTP 429, 5xx): retried with exponential backoff up to
//     maxChunkSendAttempts total attempts.
//   - Permanent errors (e.g. 400 schema mismatch): returned immediately.
//   - Transport errors (DNS, TLS, timeout): retried with backoff like transient errors.
//
// offsetToken is passed through to appendRows for Snowflake-level deduplication on
// Temporal retries; it is reused unchanged on channel-reopen retries so Snowflake
// treats the re-send as a no-op if the first attempt partially committed rows.
func (m *StreamingChannelManager) sendChunkWithRetry(
	ctx context.Context,
	db, schema, pipe, channelName string,
	ch *StreamingChannel,
	encodedRows []byte,
	offsetToken string,
) error {
	m.logger.Debug("[streaming] sending chunk",
		"channel", channelName,
		"offsetToken", offsetToken,
		"bytes", len(encodedRows),
	)

	backoff := initialBackoff

	for attempt := 0; attempt < maxChunkSendAttempts; attempt++ {
		if err := ch.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiter wait: %w", err)
		}

		errResp, err := m.appendRows(ctx, db, schema, pipe, channelName, ch, encodedRows, offsetToken)
		if err != nil {
			// Pure transport error (DNS, TLS, network timeout) — always transient.
			if attempt < maxChunkSendAttempts-1 {
				m.logger.Warn("[streaming] transport error sending chunk, retrying with backoff",
					"channel", channelName,
					"offsetToken", offsetToken,
					"attempt", attempt+1,
					"backoff", backoff,
					"err", err,
				)
				m.metrics.chunkRetriesCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
					attribute.String(snowpipeRetryReasonKey, "transport"),
				)))
				if waitErr := sleepWithContext(ctx, backoff); waitErr != nil {
					return waitErr
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			return fmt.Errorf("transport error after %d attempts on channel %s: %w", maxChunkSendAttempts, channelName, err)
		}

		if errResp == nil {
			// Success.
			m.metrics.bytesSentCounter.Add(ctx, int64(len(encodedRows)))
			return nil
		}

		m.logger.Warn("[streaming] append rows error",
			"channel", channelName,
			"code", errResp.Code,
			"httpStatus", errResp.HTTPStatus,
			"offsetToken", offsetToken,
			"attempt", attempt+1,
			"message", errResp.Message,
		)

		if errResp.IsChannelReopenError() {
			// Channel must be reopened. Reuse the same offsetToken on retry:
			// Snowflake deduplicates based on it, so re-sending is safe.
			m.metrics.chunkRetriesCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String(snowpipeRetryReasonKey, "channel_reopen"),
			)))
			ch.invalidate()
			newCh, err := m.getOrCreateChannel(ctx, db, schema, pipe, channelName, true)
			if err != nil {
				return fmt.Errorf("failed to reopen channel after %s: %w", errResp.Code, err)
			}
			ch = newCh
			continue
		}

		if errResp.IsAuthError() {
			// Token expired mid-batch. Refresh and retry with the same channel.
			m.logger.Warn("[streaming] auth error (401), refreshing token and retrying",
				"channel", channelName, "offsetToken", offsetToken)
			m.metrics.chunkRetriesCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
				attribute.String(snowpipeRetryReasonKey, "auth"),
			)))
			if refreshErr := m.refreshAuth(ctx); refreshErr != nil {
				return fmt.Errorf("failed to refresh auth after 401 on channel %s: %w", channelName, refreshErr)
			}
			// appendRows reads m.scopedToken directly, so the next attempt uses the new token.
			continue
		}

		if errResp.IsTransientError() {
			if attempt < maxChunkSendAttempts-1 {
				m.logger.Warn("[streaming] transient error, retrying with backoff",
					"channel", channelName,
					"code", errResp.Code,
					"httpStatus", errResp.HTTPStatus,
					"offsetToken", offsetToken,
					"attempt", attempt+1,
					"backoff", backoff,
				)
				m.metrics.chunkRetriesCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
					attribute.String(snowpipeRetryReasonKey, "transient"),
				)))
				if waitErr := sleepWithContext(ctx, backoff); waitErr != nil {
					return waitErr
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			return fmt.Errorf("transient error (code %s, http %d) after %d attempts on channel %s: %w",
				errResp.Code, errResp.HTTPStatus, maxChunkSendAttempts, channelName, errResp)
		}

		// Permanent error: schema mismatch, permission denied, malformed payload, etc.
		// Return immediately — retrying won't help.
		return fmt.Errorf("permanent snowpipe error (code %s, http %d) on channel %s: %w",
			errResp.Code, errResp.HTTPStatus, channelName, errResp)
	}

	return fmt.Errorf("exceeded %d retry attempts for chunk on channel %s (offsetToken %s)",
		maxChunkSendAttempts, channelName, offsetToken)
}

// sleepWithContext sleeps for d or until ctx is cancelled, whichever comes first.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// GetChannelStatus fetches per-channel ingestion metrics via the bulk channel status
// endpoint. It is used for delivery confirmation: the caller can detect ingestion errors
// (via RowsErrorCount + LastErrorMessage) and confirm committed rows (via RowsInserted).
//
// Endpoint: POST https://{ingestHost}/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}:bulk-channel-status
func (m *StreamingChannelManager) GetChannelStatus(
	ctx context.Context,
	db, schema, pipe string,
	channelNames []string,
) (map[string]ChannelStatus, error) {
	reqURL := fmt.Sprintf(
		"https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s:bulk-channel-status",
		m.ingestHost, db, schema, pipe,
	)

	type bulkPayload struct {
		ChannelNames []string `json:"channel_names"`
	}
	payloadBody, err := json.Marshal(bulkPayload{ChannelNames: channelNames})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bulk channel status payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(payloadBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk channel status request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+m.scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query bulk channel status: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read bulk channel status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("bulk channel status request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		ChannelStatuses map[string]ChannelStatus `json:"channel_statuses"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse bulk channel status response: %w", err)
	}
	return response.ChannelStatuses, nil
}

// Close invalidates all channels and clears the manager state.
// The two mutexes are released independently (channelsMu first, then refreshMu) to
// avoid holding both simultaneously and to match the lock ordering in refreshAuth.
func (m *StreamingChannelManager) Close() {
	m.channelsMu.Lock()
	for _, ch := range m.channels {
		ch.invalidate()
	}
	m.channels = make(map[string]*StreamingChannel)
	m.channelsMu.Unlock()

	// refreshMu must be held when writing scopedToken/ingestHost; refreshAuth holds it
	// too, so skipping it here would be a data race under concurrent Close + refreshAuth.
	m.refreshMu.Lock()
	m.scopedToken = ""
	m.ingestHost = ""
	m.refreshMu.Unlock()
}
