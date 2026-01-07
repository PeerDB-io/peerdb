package connclickhouse

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/model"
)

func collectBatches(ch <-chan *objectBatch) []*objectBatch {
	var batches []*objectBatch //nolint:prealloc
	for batch := range ch {
		batches = append(batches, batch)
	}
	return batches
}

func TestCollectAndBatchObjects(t *testing.T) {
	ctx := context.Background()
	longInterval := time.Hour

	t.Run("single object", func(t *testing.T) {
		stream := model.NewQObjectStream(1)
		go func() {
			stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100}
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 1000, longInterval)
		batches := collectBatches(batchCh)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].objects, 1)
		require.Equal(t, "http://example.com/1.avro", batches[0].objects[0].URL)
	})

	t.Run("multiple objects within size limit", func(t *testing.T) {
		stream := model.NewQObjectStream(3)
		headers := http.Header{"Authorization": []string{"Bearer token"}}
		go func() {
			stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100, Headers: headers}
			stream.Objects <- &model.Object{URL: "http://example.com/2.avro", Size: 100, Headers: headers}
			stream.Objects <- &model.Object{URL: "http://example.com/3.avro", Size: 100, Headers: headers}
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 1000, longInterval)
		batches := collectBatches(batchCh)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].objects, 3)
		require.Equal(t, int64(300), batches[0].size)
		require.Equal(t, headers, batches[0].headers())
	})

	t.Run("objects split by size limit", func(t *testing.T) {
		stream := model.NewQObjectStream(3)
		go func() {
			stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100}
			stream.Objects <- &model.Object{URL: "http://example.com/2.avro", Size: 100}
			stream.Objects <- &model.Object{URL: "http://example.com/3.avro", Size: 100}
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 150, longInterval)
		batches := collectBatches(batchCh)
		require.Len(t, batches, 3)
		for i, batch := range batches {
			require.Len(t, batch.objects, 1, "batch %d should have 1 object", i)
		}
	})

	t.Run("empty stream", func(t *testing.T) {
		stream := model.NewQObjectStream(1)
		go func() {
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 1000, longInterval)
		batches := collectBatches(batchCh)
		require.Empty(t, batches)
	})

	t.Run("object exactly at size limit starts new batch", func(t *testing.T) {
		stream := model.NewQObjectStream(2)
		go func() {
			stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 500}
			stream.Objects <- &model.Object{URL: "http://example.com/2.avro", Size: 500}
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 500, longInterval)
		batches := collectBatches(batchCh)
		require.Len(t, batches, 2)
	})

	t.Run("flush interval triggers batch release", func(t *testing.T) {
		stream := model.NewQObjectStream(2)
		shortInterval := 50 * time.Millisecond

		batchCh := collectAndBatchObjects(ctx, stream, 10000, shortInterval)

		stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100}

		select {
		case batch := <-batchCh:
			require.NotNil(t, batch)
			require.Len(t, batch.objects, 1)
			require.Equal(t, "http://example.com/1.avro", batch.objects[0].URL)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("expected batch to be flushed after interval")
		}

		stream.Objects <- &model.Object{URL: "http://example.com/2.avro", Size: 100}
		stream.Close(nil)

		var remaining []*objectBatch
		for batch := range batchCh {
			remaining = append(remaining, batch)
		}
		require.Len(t, remaining, 1)
		require.Equal(t, "http://example.com/2.avro", remaining[0].objects[0].URL)
	})

	t.Run("context cancellation stops batching", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream := model.NewQObjectStream(10)

		batchCh := collectAndBatchObjects(ctx, stream, 10000, longInterval)

		stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100}

		cancel()

		select {
		case _, _ = <-batchCh: //nolint:staticcheck
			// Channel closed or got batch, both are acceptable
		case <-time.After(100 * time.Millisecond):
			t.Fatal("channel should be closed after context cancellation")
		}
	})

	t.Run("batch uses latest object headers", func(t *testing.T) {
		stream := model.NewQObjectStream(3)
		headers1 := http.Header{"Authorization": []string{"Bearer token1"}}
		headers2 := http.Header{"Authorization": []string{"Bearer token2"}}
		headers3 := http.Header{"Authorization": []string{"Bearer token3"}}
		go func() {
			stream.Objects <- &model.Object{URL: "http://example.com/1.avro", Size: 100, Headers: headers1}
			stream.Objects <- &model.Object{URL: "http://example.com/2.avro", Size: 100, Headers: headers2}
			stream.Objects <- &model.Object{URL: "http://example.com/3.avro", Size: 100, Headers: headers3}
			stream.Close(nil)
		}()

		batchCh := collectAndBatchObjects(ctx, stream, 1000, longInterval)
		batches := collectBatches(batchCh)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].objects, 3)
		require.Equal(t, headers3, batches[0].headers())
	})
}

func TestBuildURLTableFunction(t *testing.T) {
	c := &ClickHouseConnector{}

	t.Run("single URL no headers", func(t *testing.T) {
		batch := &objectBatch{
			objects: []*model.Object{
				{URL: "http://example.com/data.avro"},
			},
		}
		result := c.buildURLTableFunction(batch, "Avro")
		require.Equal(t, "url('http://example.com/data.avro', 'Avro')", result)
	})

	t.Run("single URL with headers", func(t *testing.T) {
		batch := &objectBatch{
			objects: []*model.Object{
				{URL: "http://example.com/data.avro", Headers: http.Header{"Authorization": []string{"Bearer token123"}}},
			},
		}
		result := c.buildURLTableFunction(batch, "Avro")
		require.Equal(t, "url('http://example.com/data.avro', headers(`Authorization`='Bearer token123'), 'Avro')", result)
	})

	t.Run("multiple URLs no headers", func(t *testing.T) {
		batch := &objectBatch{
			objects: []*model.Object{
				{URL: "http://example.com/1.avro"},
				{URL: "http://example.com/2.avro"},
				{URL: "http://example.com/3.avro"},
			},
		}
		result := c.buildURLTableFunction(batch, "Avro")
		require.Equal(t, "url('{http://example.com/1.avro,http://example.com/2.avro,http://example.com/3.avro}', 'Avro')", result)
	})

	t.Run("multiple URLs with headers", func(t *testing.T) {
		batch := &objectBatch{
			objects: []*model.Object{
				{URL: "http://example.com/1.avro", Headers: http.Header{"Authorization": []string{"Bearer token_1"}}},
				{URL: "http://example.com/2.avro", Headers: http.Header{"Authorization": []string{"Bearer token_2"}}},
			},
		}
		result := c.buildURLTableFunction(batch, "Avro")
		require.Equal(
			t,
			"url('{http://example.com/1.avro,http://example.com/2.avro}', headers(`Authorization`='Bearer token_2'), 'Avro')",
			result,
		)
	})

	t.Run("URL with special characters", func(t *testing.T) {
		batch := &objectBatch{
			objects: []*model.Object{
				{URL: "http://example.com/path with spaces/data.avro"},
			},
		}
		result := c.buildURLTableFunction(batch, "Avro")
		require.Equal(t, "url('http://example.com/path with spaces/data.avro', 'Avro')", result)
	})
}
