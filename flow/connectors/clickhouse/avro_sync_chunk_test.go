package connclickhouse

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type recordingStagingStore struct {
	keys  []string
	files [][]byte
}

func (s *recordingStagingStore) Upload(_ context.Context, _ map[string]string, key string, body io.Reader) error {
	contents, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.keys = append(s.keys, key)
	s.files = append(s.files, contents)
	return nil
}

func (*recordingStagingStore) TableFunctionExpr(context.Context, string, string) (string, error) {
	return "", nil
}

func (*recordingStagingStore) DeletePrefix(context.Context, string) error { return nil }
func (*recordingStagingStore) Validate(context.Context) error             { return nil }
func (*recordingStagingStore) ClickHouseAccessMethod() string             { return "S3" }
func (*recordingStagingStore) BucketPath() string                         { return "s3://test/stage" }
func (*recordingStagingStore) KeyPrefix() string                          { return "stage" }

func TestWriteToAvroFilesChunksRecords(t *testing.T) {
	ctx := t.Context()
	env := map[string]string{
		"PEERDB_AVRO_NULLABLE_LAX":                      "false",
		"PEERDB_CLICKHOUSE_BINARY_FORMAT":               "raw",
		"PEERDB_CLICKHOUSE_UNBOUNDED_NUMERIC_AS_STRING": "false",
		"PEERDB_NULLABLE":                               "false",
		"PEERDB_S3_BYTES_PER_AVRO_FILE":                 "1",
		"PEERDB_S3_UUID_PREFIX":                         "false",
	}
	schema := types.QRecordSchema{Fields: []types.QField{{
		Name: "value", Type: types.QValueKindString,
	}}}
	stream := model.NewQRecordStream(3)
	stream.SetSchema(schema)
	stream.Records <- []types.QValue{types.QValueString{Val: "first"}}
	stream.Records <- []types.QValue{types.QValueString{Val: "second"}}
	stream.Records <- []types.QValue{types.QValueString{Val: "third"}}
	stream.Close(nil)

	avroSchema, err := model.GetAvroSchemaDefinition(ctx, env, "dst", schema, protos.DBType_CLICKHOUSE, nil)
	require.NoError(t, err)

	staging := &recordingStagingStore{}
	syncer := NewClickHouseAvroSyncMethod(&protos.QRepConfig{Env: env}, &ClickHouseConnector{
		logger:  internal.LoggerFromCtx(ctx),
		staging: staging,
	})
	files, totalRecords, err := syncer.writeToAvroFiles(
		ctx, env, stream, avroSchema, "batch", "flow", nil, nil,
	)
	require.NoError(t, err)
	require.EqualValues(t, 3, totalRecords)
	require.Len(t, files, 3)
	require.GreaterOrEqual(t, len(staging.files), 3)
	require.Equal(t, []string{
		"stage/flow/batch.000000.avro",
		"stage/flow/batch.000001.avro",
		"stage/flow/batch.000002.avro",
	}, staging.keys[:3])
	for _, file := range files {
		require.EqualValues(t, 1, file.NumRecords)
	}
}
