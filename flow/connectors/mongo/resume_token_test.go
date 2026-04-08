package connmongo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeTimestampFromKeyString(t *testing.T) {
	// expected 'T' and 'I' can be derived in mongosh:
	//     snippet install resumetoken
	//     decodeResumeToken('<RESUME TOKEN>')
	//nolint:lll
	ts, err := decodeTimestampFromKeyString("82687F3418000000012B042C0100296E5A100402029C35AFFD457AA3093B44F7D71C6D463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F69640064687F3418245CA5A3B5F47124000004")
	require.NoError(t, err)
	require.Equal(t, uint32(1753166872), ts.T)
	require.Equal(t, uint32(1), ts.I)
}
