import { ListPeersResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import CreateMirrors from './CreateMirrors';

function fetchPeers(): Promise<ListPeersResponse> {
  const addr = GetFlowHttpAddressFromEnv();
  return fetch(`${addr}/v1/peers/list`, { cache: 'no-store' }).then((res) =>
    res.json()
  );
}

export default function CreateMirrorsPage() {
  const peersPromise = fetchPeers();
  return <CreateMirrors peersPromise={peersPromise} />;
}
