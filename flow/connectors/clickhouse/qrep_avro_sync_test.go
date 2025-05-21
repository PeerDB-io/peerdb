package connclickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

func TestCastToDestinationTypeIfSupported(t *testing.T) {
	str := castToDestinationTypeIfSupported("foo", qvalue.QValueKindNumeric, "String")
	assert.Equal(t, "CAST(`foo`, 'String')", str)

	// empty
	str = castToDestinationTypeIfSupported("foo", qvalue.QValueKindNumeric, "")
	assert.Equal(t, "`foo`", str)

	// unsupported
	str = castToDestinationTypeIfSupported("foo", qvalue.QValueKindNumeric, "FooBar")
	assert.Equal(t, "`foo`", str)
}
