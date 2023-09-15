package e2e_postgres

import "github.com/PeerDB-io/peer-flow/generated/protos"

func generatePGPeer(postgresConfig *protos.PostgresConfig) *protos.Peer {
	ret := &protos.Peer{}
	ret.Name = "test_pg_peer"
	ret.Type = protos.DBType_POSTGRES

	ret.Config = &protos.Peer_PostgresConfig{
		PostgresConfig: postgresConfig,
	}

	return ret
}
