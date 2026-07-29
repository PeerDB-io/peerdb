//go:build ruleguard

package gorules

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

// Enforce FlowConnectionConfigsCore usage outside cmd/ and e2e/
func flowConfigUsage(m dsl.Matcher) {
	m.ImportAs("github.com/PeerDB-io/peerdb/flow/generated/protos", "protos")
	m.Match(
		"*protos.FlowConnectionConfigs",     // pointer type
		"protos.FlowConnectionConfigs",      // non-pointer type
		"protos.FlowConnectionConfigs{$*_}", // struct literal
	).
		Where(!m.File().PkgPath.Matches(`github\.com/PeerDB-io/peerdb/flow/cmd`) &&
			!m.File().PkgPath.Matches(`github\.com/PeerDB-io/peerdb/flow/e2e`)).
		Report("Use *protos.FlowConnectionConfigsCore instead of FlowConnectionConfigs outside cmd/ and e2e/ packages")
}
