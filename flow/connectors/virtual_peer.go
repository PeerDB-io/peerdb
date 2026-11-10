package connectors

import (
	"context"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// A virtual peer name "<base>__<database>" stands for the ClickHouse peer <base>
// writing to <database>. It has no catalog row: it is resolved from the base peer
// on every load, so credentials always come from the base peer. Mirrors use it to
// choose a destination database without creating a copy of the peer.
const VirtualPeerSeparator = "__"

var virtualPeerDatabaseRegexp = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// SplitVirtualPeerName returns the base peer name and database of a virtual peer name.
// The base is everything before the first separator, so it never contains one itself.
func SplitVirtualPeerName(name string) (string, string, bool) {
	base, database, found := strings.Cut(name, VirtualPeerSeparator)
	if !found || base == "" || !virtualPeerDatabaseRegexp.MatchString(database) {
		return "", "", false
	}
	return base, database, true
}

// VirtualPeerName builds the virtual peer name for a ClickHouse peer and database.
func VirtualPeerName(base string, database string) string {
	return base + VirtualPeerSeparator + database
}

// resolveVirtualPeer builds the virtual peer `name` from its base ClickHouse peer.
// It returns nil when `name` is not a virtual peer of an existing ClickHouse peer.
func resolveVirtualPeer(ctx context.Context, catalogPool shared.CatalogPool, name string) (*protos.Peer, error) {
	base, database, ok := SplitVirtualPeerName(name)
	if !ok {
		return nil, nil
	}
	basePeers, err := loadCatalogPeers(ctx, catalogPool, []string{base})
	if err != nil {
		return nil, err
	}
	basePeer, found := basePeers[base]
	if !found || basePeer.GetClickhouseConfig() == nil {
		return nil, nil
	}
	peer := proto.Clone(basePeer).(*protos.Peer)
	peer.Name = name
	peer.GetClickhouseConfig().Database = database
	return peer, nil
}
