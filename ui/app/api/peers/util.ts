import { PeerConfig } from '@/app/peers/create/configuration/types';
import { DBType, Peer, PostgresConfig } from '@/grpc_generated/peers';

export const constructPeer = (
  name: string,
  type: string,
  config: PeerConfig
): Peer | undefined => {
  switch (type) {
    case 'POSTGRES':
      return {
        name,
        type: DBType.POSTGRES,
        postgresConfig: config as PostgresConfig,
      };
    default:
      return;
  }
};
