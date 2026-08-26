//go:build ruleguard

package gorules

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

// Enforce FlowConnectionConfigsCore usage outside cmd/, e2e/ and proto_conversions/
func flowConfigUsage(m dsl.Matcher) {
	m.ImportAs("github.com/PeerDB-io/peerdb/flow/generated/protos", "protos")
	m.Match(
		"*protos.FlowConnectionConfigs",     // pointer type
		"protos.FlowConnectionConfigs",      // non-pointer type
		"protos.FlowConnectionConfigs{$*_}", // struct literal
	).
		Where(!m.File().PkgPath.Matches(`github\.com/PeerDB-io/peerdb/flow/cmd`) &&
			!m.File().PkgPath.Matches(`github\.com/PeerDB-io/peerdb/flow/e2e`) &&
			!m.File().PkgPath.Matches(`github\.com/PeerDB-io/peerdb/flow/proto_conversions`)).
		Report("Use *protos.FlowConnectionConfigsCore instead of FlowConnectionConfigs outside cmd/, e2e/ and proto_conversions/ packages")
}
