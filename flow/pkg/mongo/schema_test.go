package mongo

import (
	"fmt"
	"math"
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func rawValueOf(t *testing.T, value any) bsoncore.Value {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "a", Value: value}})
	require.NoError(t, err)
	rv := bson.Raw(raw).Lookup("a")
	return bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}
}

func TestRawValueToJSONFloatLimitBoundary(t *testing.T) {
	stream := jsoniter.NewStream(jsoniter.ConfigCompatibleWithStandardLibrary, nil, 64)
	for _, value := range []float64{
		floatLimit, math.Nextafter(floatLimit, math.Inf(1)), math.Nextafter(floatLimit, math.Inf(-1)),
		floatNegLimit, math.Nextafter(floatNegLimit, math.Inf(1)), math.Nextafter(floatNegLimit, math.Inf(-1)),
	} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			stream.Reset(nil)
			require.NoError(t, RawValueToJSON(rawValueOf(t, value), stream))
			result := string(stream.Buffer())
			require.Less(t, len(result), 31)
			require.True(t, strings.Contains(result, ".") || strings.Contains(result, "e"), result)
		})
	}
}
