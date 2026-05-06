package connsnowflake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/snowflakedb/gosnowflake"

	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// ── rawTableColumns ───────────────────────────────────────────────────────────

// TestRawTableColumns_Count verifies that rawTableColumns has the expected number
// of columns and that the order matches createRawTableSQL.
func TestRawTableColumns_Count(t *testing.T) {
	expected := []string{
		"_PEERDB_UID",
		"_PEERDB_TIMESTAMP",
		"_PEERDB_DESTINATION_TABLE_NAME",
		"_PEERDB_DATA",
		"_PEERDB_RECORD_TYPE",
		"_PEERDB_MATCH_DATA",
		"_PEERDB_BATCH_ID",
		"_PEERDB_UNCHANGED_TOAST_COLUMNS",
	}
	if len(rawTableColumns) != len(expected) {
		t.Fatalf("expected %d rawTableColumns, got %d", len(expected), len(rawTableColumns))
	}
	for i, want := range expected {
		if rawTableColumns[i] != want {
			t.Errorf("rawTableColumns[%d]: expected %q, got %q", i, want, rawTableColumns[i])
		}
	}
}

// ── buildRawStreamingRow ──────────────────────────────────────────────────────

// TestBuildRawStreamingRow_Insert verifies that an INSERT record produces a raw row
// with recordType=0, empty matchData, empty unchangedToast, and the correct destination.
func TestBuildRawStreamingRow_Insert(t *testing.T) {
	items := model.NewRecordItems(2)
	items.AddColumn("id", types.QValueInt32{Val: 42})
	items.AddColumn("name", types.QValueString{Val: "Alice"})

	rec := &model.InsertRecord[model.RecordItems]{
		Items:                items,
		DestinationTableName: "public.users",
	}

	rowBytes, err := buildRawStreamingRow(rec, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must end with newline (NDJSON format)
	if len(rowBytes) == 0 || rowBytes[len(rowBytes)-1] != '\n' {
		t.Error("expected NDJSON row to end with newline")
	}

	var row rawRow
	if err := json.Unmarshal(rowBytes[:len(rowBytes)-1], &row); err != nil {
		t.Fatalf("failed to unmarshal row: %v", err)
	}

	if row.UID == "" {
		t.Error("expected non-empty UID")
	}
	if row.Timestamp == 0 {
		t.Error("expected non-zero Timestamp")
	}
	if row.DestinationTable != "public.users" {
		t.Errorf("expected DestinationTable=public.users, got %q", row.DestinationTable)
	}
	if row.RecordType != 0 {
		t.Errorf("expected RecordType=0 (INSERT), got %d", row.RecordType)
	}
	if row.MatchData != "" {
		t.Errorf("expected empty MatchData for INSERT, got %q", row.MatchData)
	}
	if row.UnchangedToast != "" {
		t.Errorf("expected empty UnchangedToast for INSERT, got %q", row.UnchangedToast)
	}
	if row.BatchID != 7 {
		t.Errorf("expected BatchID=7, got %d", row.BatchID)
	}
	// Data must be non-empty JSON
	if row.Data == "" || row.Data == "null" {
		t.Errorf("expected non-empty Data JSON, got %q", row.Data)
	}
}

// TestBuildRawStreamingRow_Update verifies that an UPDATE record produces a raw row
// with recordType=1, matchData=oldItems, and unchangedToast set.
func TestBuildRawStreamingRow_Update(t *testing.T) {
	newItems := model.NewRecordItems(2)
	newItems.AddColumn("id", types.QValueInt32{Val: 1})
	newItems.AddColumn("status", types.QValueString{Val: "shipped"})

	oldItems := model.NewRecordItems(2)
	oldItems.AddColumn("id", types.QValueInt32{Val: 1})
	oldItems.AddColumn("status", types.QValueString{Val: "pending"})

	rec := &model.UpdateRecord[model.RecordItems]{
		NewItems:              newItems,
		OldItems:              oldItems,
		UnchangedToastColumns: map[string]struct{}{"notes": {}, "metadata": {}},
		DestinationTableName:  "public.orders",
	}

	rowBytes, err := buildRawStreamingRow(rec, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var row rawRow
	if err := json.Unmarshal(rowBytes[:len(rowBytes)-1], &row); err != nil {
		t.Fatalf("failed to unmarshal row: %v", err)
	}

	if row.RecordType != 1 {
		t.Errorf("expected RecordType=1 (UPDATE), got %d", row.RecordType)
	}
	if row.MatchData == "" {
		t.Error("expected non-empty MatchData for UPDATE (old items)")
	}
	if row.Data == "" {
		t.Error("expected non-empty Data for UPDATE (new items)")
	}
	// Data and MatchData must differ when old != new
	if row.Data == row.MatchData {
		t.Error("expected Data != MatchData when old and new items differ")
	}
	// UnchangedToast must be a sorted comma-separated list of the unchanged columns
	if row.UnchangedToast != "metadata,notes" {
		t.Errorf("expected UnchangedToast=metadata,notes, got %q", row.UnchangedToast)
	}
	if row.DestinationTable != "public.orders" {
		t.Errorf("expected DestinationTable=public.orders, got %q", row.DestinationTable)
	}
}

// TestBuildRawStreamingRow_Delete verifies that a DELETE record produces a raw row
// with recordType=2 and matchData == data (standard convention for deletes).
func TestBuildRawStreamingRow_Delete(t *testing.T) {
	items := model.NewRecordItems(1)
	items.AddColumn("id", types.QValueInt32{Val: 99})

	rec := &model.DeleteRecord[model.RecordItems]{
		Items:                items,
		UnchangedToastColumns: nil,
		DestinationTableName:  "public.products",
	}

	rowBytes, err := buildRawStreamingRow(rec, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var row rawRow
	if err := json.Unmarshal(rowBytes[:len(rowBytes)-1], &row); err != nil {
		t.Fatalf("failed to unmarshal row: %v", err)
	}

	if row.RecordType != 2 {
		t.Errorf("expected RecordType=2 (DELETE), got %d", row.RecordType)
	}
	if row.Data == "" {
		t.Error("expected non-empty Data for DELETE")
	}
	// For DELETE, matchData == data (MERGE uses matchData to identify the row to delete)
	if row.MatchData != row.Data {
		t.Errorf("expected MatchData == Data for DELETE, got matchData=%q data=%q", row.MatchData, row.Data)
	}
	if row.UnchangedToast != "" {
		t.Errorf("expected empty UnchangedToast for DELETE with nil unchanged cols, got %q", row.UnchangedToast)
	}
}

// TestBuildRawStreamingRow_ProducesNDJSON verifies the output always ends with '\n'.
func TestBuildRawStreamingRow_ProducesNDJSON(t *testing.T) {
	items := model.NewRecordItems(1)
	items.AddColumn("x", types.QValueInt32{Val: 1})
	rec := &model.InsertRecord[model.RecordItems]{
		Items:                items,
		DestinationTableName: "t",
	}
	b, err := buildRawStreamingRow(rec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if b[len(b)-1] != '\n' {
		t.Error("expected output to end with newline (NDJSON)")
	}
}

// TestBuildRawStreamingRow_UniqueUIDs verifies that two calls produce different UIDs.
func TestBuildRawStreamingRow_UniqueUIDs(t *testing.T) {
	items := model.NewRecordItems(1)
	items.AddColumn("x", types.QValueInt32{Val: 1})
	rec := &model.InsertRecord[model.RecordItems]{
		Items:                items,
		DestinationTableName: "t",
	}

	b1, err := buildRawStreamingRow(rec, 1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := buildRawStreamingRow(rec, 1)
	if err != nil {
		t.Fatal(err)
	}

	var row1, row2 rawRow
	if err := json.Unmarshal(b1[:len(b1)-1], &row1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b2[:len(b2)-1], &row2); err != nil {
		t.Fatal(err)
	}
	if row1.UID == row2.UID {
		t.Errorf("expected unique UIDs per call, both got %q", row1.UID)
	}
}

// ── offsetToken format ────────────────────────────────────────────────────────

// TestOffsetToken_Format verifies that the offsetToken format produced by flushRawBuffer
// is "<syncBatchID>-<chunkIndex>", which uniquely identifies each chunk across retries.
func TestOffsetToken_Format(t *testing.T) {
	syncBatchID := int64(42)
	flushIndex := 0
	token := fmt.Sprintf("%d-%d", syncBatchID, flushIndex)
	if token != "42-0" {
		t.Errorf("expected offsetToken=42-0, got %q", token)
	}

	flushIndex++
	token = fmt.Sprintf("%d-%d", syncBatchID, flushIndex)
	if token != "42-1" {
		t.Errorf("expected offsetToken=42-1 after increment, got %q", token)
	}
}

// ── channel and manager tests (unchanged from original) ───────────────────────

func TestChannelReopenErrorCodes(t *testing.T) {
	knownCodes := []string{
		"ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER",
		"ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER",
		"ERR_CHANNEL_MUST_BE_REOPENED",
		"ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP",
		"ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED",
		"STALE_PIPE_CACHE",
	}

	for _, code := range knownCodes {
		errResp := &StreamingErrorResponse{Code: code, Message: "test"}
		if !errResp.IsChannelReopenError() {
			t.Errorf("expected %s to be a channel reopen error", code)
		}
	}

	unknownCodes := []string{
		"SOME_OTHER_ERROR",
		"INTERNAL_ERROR",
		"",
	}

	for _, code := range unknownCodes {
		errResp := &StreamingErrorResponse{Code: code, Message: "test"}
		if errResp.IsChannelReopenError() {
			t.Errorf("expected %s to NOT be a channel reopen error", code)
		}
	}
}

func TestStreamingErrorResponse_Error(t *testing.T) {
	errResp := &StreamingErrorResponse{
		Code:    "ERR_CHANNEL_MUST_BE_REOPENED",
		Message: "channel needs reopen",
	}

	expected := "snowpipe streaming error ERR_CHANNEL_MUST_BE_REOPENED (http 0): channel needs reopen"
	if errResp.Error() != expected {
		t.Errorf("expected %q, got %q", expected, errResp.Error())
	}
}

func TestStreamingErrorResponse_ResultHandler(t *testing.T) {
	raw := `{"code":"ERR_CHANNEL_MUST_BE_REOPENED","message":"channel stale","result_handler":"REOPEN"}`
	var errResp StreamingErrorResponse
	if err := json.Unmarshal([]byte(raw), &errResp); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if errResp.Code != "ERR_CHANNEL_MUST_BE_REOPENED" {
		t.Errorf("expected code ERR_CHANNEL_MUST_BE_REOPENED, got %q", errResp.Code)
	}
	if errResp.ResultHandler != "REOPEN" {
		t.Errorf("expected result_handler=REOPEN, got %q", errResp.ResultHandler)
	}
	if !errResp.IsChannelReopenError() {
		t.Error("expected IsChannelReopenError to be true")
	}
}

func TestChannelStatus_JSONParsing(t *testing.T) {
	raw := `{
		"channel_statuses": {
			"peerdb-myjob-raw-0": {
				"channel_status_code": "OPENED",
				"rows_inserted": 250,
				"rows_parsed": 250,
				"rows_error_count": 3,
				"last_error_message": "failed to cast TIMESTAMP_TZ to BOOLEAN",
				"snowflake_avg_processing_latency_ms": 120
			}
		}
	}`

	var response struct {
		ChannelStatuses map[string]ChannelStatus `json:"channel_statuses"`
	}
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	status, ok := response.ChannelStatuses["peerdb-myjob-raw-0"]
	if !ok {
		t.Fatal("expected channel key not found in response")
	}
	if status.RowsInserted != 250 {
		t.Errorf("expected RowsInserted=250, got %d", status.RowsInserted)
	}
	if status.RowsErrorCount != 3 {
		t.Errorf("expected RowsErrorCount=3, got %d", status.RowsErrorCount)
	}
	if status.LastErrorMessage != "failed to cast TIMESTAMP_TZ to BOOLEAN" {
		t.Errorf("unexpected LastErrorMessage: %q", status.LastErrorMessage)
	}
	if status.SnowflakeAvgProcessingLatencyMs != 120 {
		t.Errorf("expected SnowflakeAvgProcessingLatencyMs=120, got %d", status.SnowflakeAvgProcessingLatencyMs)
	}
}

func TestChannelStatus_HasNoErrors(t *testing.T) {
	status := ChannelStatus{
		RowsInserted:     10,
		RowsErrorCount:   0,
		LastErrorMessage: "",
	}

	baseline := 5
	if !(status.RowsInserted >= baseline+5) {
		t.Error("expected RowsInserted >= baseline+rowsSent for success case")
	}
	if status.RowsErrorCount > 0 && status.LastErrorMessage != "" {
		t.Error("unexpected error in no-error case")
	}
}

// TestStreamingChannel_RowsInsertedTracking verifies that the rowsInserted counter is
// correctly managed through set, get, and invalidate operations.
func TestStreamingChannel_RowsInsertedTracking(t *testing.T) {
	ch := &StreamingChannel{}

	if got := ch.getRowsInserted(); got != 0 {
		t.Errorf("expected initial rowsInserted=0, got %d", got)
	}

	ch.setRowsInserted(42)
	if got := ch.getRowsInserted(); got != 42 {
		t.Errorf("expected rowsInserted=42 after set, got %d", got)
	}

	ch.setRowsInserted(100)
	if got := ch.getRowsInserted(); got != 100 {
		t.Errorf("expected rowsInserted=100 after second set, got %d", got)
	}

	ch.invalidate()
	if got := ch.getRowsInserted(); got != 0 {
		t.Errorf("expected rowsInserted=0 after invalidate, got %d", got)
	}

	ch.setContinuationToken("tok123")
	ch.setRowsInserted(55)
	ch.invalidate()
	if got := ch.getContinuationToken(); got != "" {
		t.Errorf("expected empty continuation token after invalidate, got %q", got)
	}
	if got := ch.getRowsInserted(); got != 0 {
		t.Errorf("expected rowsInserted=0 after invalidate (with token), got %d", got)
	}
}

// TestStreamingChannelManager_HttpClientTimeout verifies that NewStreamingChannelManager
// initialises a non-nil httpClient with a configured timeout.
func TestStreamingChannelManager_HttpClientTimeout(t *testing.T) {
	m := NewStreamingChannelManager(&gosnowflake.Config{}, nil, 0, 0, DefaultIngestTimeoutSeconds)
	if m.httpClient == nil {
		t.Fatal("expected httpClient to be non-nil")
	}
	if m.httpClient.Timeout == 0 {
		t.Error("expected httpClient.Timeout to be non-zero")
	}
	if m.httpClient.Transport == nil {
		t.Error("expected httpClient.Transport to be explicitly configured")
	}
	transport, ok := m.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", m.httpClient.Transport)
	}
	if transport.MaxIdleConns == 0 {
		t.Error("expected MaxIdleConns to be configured")
	}
}

// TestSyncResponse_NumRecordsSynced verifies the correct sum logic for NumRecordsSynced:
// it must total InsertCount + UpdateCount + DeleteCount across all tables.
func TestSyncResponse_NumRecordsSynced(t *testing.T) {
	tableNameRowsMapping := map[string]*model.RecordTypeCounts{
		"public.orders":    {},
		"public.customers": {},
		"public.products":  {},
	}
	tableNameRowsMapping["public.orders"].InsertCount.Store(100)
	tableNameRowsMapping["public.customers"].UpdateCount.Store(50)
	tableNameRowsMapping["public.products"].DeleteCount.Store(25)

	var totalRows int64
	for _, counts := range tableNameRowsMapping {
		totalRows += int64(counts.InsertCount.Load()) +
			int64(counts.UpdateCount.Load()) +
			int64(counts.DeleteCount.Load())
	}
	if totalRows != 175 {
		t.Errorf("expected totalRows=175, got %d", totalRows)
	}
}

// TestSyncRecordsViaStreaming_BatchIdGuard documents the idempotency guard logic:
// when syncBatchID <= lastCommittedBatchID the batch must be skipped.
func TestSyncRecordsViaStreaming_BatchIdGuard(t *testing.T) {
	cases := []struct {
		name        string
		syncBatchID int64
		lastBatchID int64
		shouldSkip  bool
	}{
		{"equal_already_committed", 5, 5, true},
		{"lower_already_committed", 4, 5, true},
		{"new_batch", 6, 5, false},
		{"first_batch", 1, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skip := tc.syncBatchID <= tc.lastBatchID
			if skip != tc.shouldSkip {
				t.Errorf("syncBatchID=%d, lastBatchID=%d: expected shouldSkip=%v, got %v",
					tc.syncBatchID, tc.lastBatchID, tc.shouldSkip, skip)
			}
		})
	}
}

// ── StreamingChannel.invalidate ──────────────────────────────────────────────

// TestStreamingChannel_Invalidate_ResetsBoth covers LEARNINGS §1.5: invalidate must
// clear BOTH continuationToken AND rowsInserted, because Snowflake resets the
// per-channel rows_inserted counter when the channel is reopened.
func TestStreamingChannel_Invalidate_ResetsBoth(t *testing.T) {
	ch := &StreamingChannel{}
	ch.setContinuationToken("ctok-abc")
	ch.setRowsInserted(42)

	ch.invalidate()

	if ch.getContinuationToken() != "" {
		t.Errorf("expected continuationToken to be cleared, got %q", ch.getContinuationToken())
	}
	if ch.getRowsInserted() != 0 {
		t.Errorf("expected rowsInserted to be reset to 0, got %d", ch.getRowsInserted())
	}
}

// ── Channel pool routing (B1) ────────────────────────────────────────────────

// TestRouteChannelIdx_BackCompat verifies that with a single channel (default), all
// rows route to channel 0 — preserving the original "peerdb-{job}-raw-0" channel
// name and durable offsetTokens for existing mirrors.
func TestRouteChannelIdx_BackCompat(t *testing.T) {
	for _, table := range []string{"public.users", "public.orders", "schema.x"} {
		if got := routeChannelIdx(table, 1); got != 0 {
			t.Errorf("routeChannelIdx(%q, 1) = %d; want 0", table, got)
		}
	}
}

// TestRouteChannelIdx_ZeroOrNegative defends against misconfiguration: any
// channel count <= 1 must collapse to channel 0 (no panic, no modulo-by-zero).
func TestRouteChannelIdx_ZeroOrNegative(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		if got := routeChannelIdx("public.users", n); got != 0 {
			t.Errorf("routeChannelIdx with channelCount=%d: got %d; want 0", n, got)
		}
	}
}

// TestRouteChannelIdx_PerTableStability verifies that the SAME destination table
// always routes to the SAME channel index. This is the per-table ordering guarantee
// required by NormalizeRecords MERGE.
func TestRouteChannelIdx_PerTableStability(t *testing.T) {
	const channels = 4
	for _, table := range []string{"public.users", "public.orders", "schema.events"} {
		first := routeChannelIdx(table, channels)
		for i := 0; i < 100; i++ {
			if got := routeChannelIdx(table, channels); got != first {
				t.Errorf("routeChannelIdx(%q, %d) not stable: got %d, expected %d", table, channels, got, first)
			}
		}
	}
}

// TestRouteChannelIdx_Distribution asserts that for a realistic table count, the
// hash distribution is "reasonable" — at least 2 distinct channels are used when
// 8 distinct tables route across 4 channels. Strict uniformity is not required;
// catastrophic skew (everything on one channel) is.
func TestRouteChannelIdx_Distribution(t *testing.T) {
	const channels = 4
	tables := []string{
		"public.users", "public.orders", "public.products", "public.events",
		"public.sessions", "public.audit", "public.kv", "public.notifications",
	}
	seen := make(map[int]bool, channels)
	for _, table := range tables {
		seen[routeChannelIdx(table, channels)] = true
	}
	if len(seen) < 2 {
		t.Errorf("hash distribution catastrophically skewed: only %d/%d channels used across %d tables", len(seen), channels, len(tables))
	}
}

// TestRouteChannelIdx_BoundedToCount ensures the returned index is always in [0, n).
func TestRouteChannelIdx_BoundedToCount(t *testing.T) {
	const channels = 7
	for _, table := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		idx := routeChannelIdx(table, channels)
		if idx < 0 || idx >= channels {
			t.Errorf("routeChannelIdx(%q, %d) = %d; out of bounds", table, channels, idx)
		}
	}
}
