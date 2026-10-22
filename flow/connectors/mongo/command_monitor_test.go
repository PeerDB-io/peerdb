package connmongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type recordedLog struct {
	attrs map[string]slog.Value
	msg   string
}

type recordingLogger struct {
	logs []recordedLog
}

func (l *recordingLogger) record(msg string, keyvals ...any) {
	attrs := make(map[string]slog.Value, len(keyvals))
	for _, kv := range keyvals {
		attr := kv.(slog.Attr)
		attrs[attr.Key] = attr.Value
	}
	l.logs = append(l.logs, recordedLog{msg: msg, attrs: attrs})
}

func (l *recordingLogger) Debug(msg string, keyvals ...any) { l.record(msg, keyvals...) }
func (l *recordingLogger) Info(msg string, keyvals ...any)  { l.record(msg, keyvals...) }
func (l *recordingLogger) Warn(msg string, keyvals ...any)  { l.record(msg, keyvals...) }
func (l *recordingLogger) Error(msg string, keyvals ...any) { l.record(msg, keyvals...) }

const secretValue = "customer-secret"

func emitCommandEvents(t *testing.T, ctx context.Context, monitor *event.CommandMonitor) {
	t.Helper()
	reply, err := bson.Marshal(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "nextBatch", Value: bson.A{bson.D{
				{Key: "_id", Value: bson.D{{Key: "_data", Value: secretValue}}},
				{Key: "fullDocument", Value: bson.D{{Key: "name", Value: secretValue}}},
			}}},
			{Key: "postBatchResumeToken", Value: bson.D{{Key: "_data", Value: secretValue}}},
			{Key: "id", Value: int64(42)},
			{Key: "ns", Value: "db.coll"},
		}},
		{Key: "ok", Value: 1.0},
		{Key: "operationTime", Value: bson.Timestamp{T: 100, I: 7}},
	})
	require.NoError(t, err)

	base := event.CommandFinishedEvent{
		CommandName:  "getMore",
		DatabaseName: "db",
		RequestID:    1,
		ConnectionID: "host:27017[-1]",
		Duration:     time.Second,
	}
	monitor.Succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: base, Reply: reply})
	monitor.Failed(ctx, &event.CommandFailedEvent{CommandFinishedEvent: base, Failure: errors.New("timed out")})
}

func TestCommandMonitorSkipsUnmarkedContext(t *testing.T) {
	logger := &recordingLogger{}
	emitCommandEvents(t, t.Context(), newCommandMonitor(logger))
	require.Empty(t, logger.logs)
}

func TestCommandMonitorLogsMetadataOnly(t *testing.T) {
	logger := &recordingLogger{}
	emitCommandEvents(t, withCommandLogging(t.Context()), newCommandMonitor(logger))

	require.Len(t, logger.logs, 2)
	require.Equal(t, "[CommandMonitor] succeeded", logger.logs[0].msg)
	require.Equal(t, "[CommandMonitor] failed", logger.logs[1].msg)

	succeeded := logger.logs[0].attrs
	require.Equal(t, "getMore", succeeded["command"].String())
	require.Equal(t, int64(1), succeeded["batchLen"].Int64())
	require.Equal(t, "timed out", fmt.Sprint(logger.logs[1].attrs["error"].Any()))

	for _, log := range logger.logs {
		for key, value := range log.attrs {
			require.NotContains(t, value.String(), secretValue, "%s logged customer data in %s", log.msg, key)
		}
	}
}
