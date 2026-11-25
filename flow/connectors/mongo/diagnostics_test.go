package connmongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestAggregateQuery(t *testing.T) {
	query, err := aggregateQuery("admin", mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "comment", Value: bson.D{{Key: "$regex", Value: "^PeerDB"}}}}}},
		{{Key: "$project", Value: bson.D{{Key: "opid", Value: 1}, {Key: "active", Value: true}}}},
	})
	require.NoError(t, err)
	require.Equal(t, `db.getSiblingDB("admin").aggregate([
  {
    "$match": {
      "comment": {
        "$regex": "^PeerDB"
      }
    }
  },
  {
    "$project": {
      "opid": 1,
      "active": true
    }
  }
])`, query)
}
