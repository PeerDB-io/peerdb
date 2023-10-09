import { ListPeersRequest } from '@/grpc_generated/route';
import { GetFlowServiceClientFromEnv } from '@/rpc/rpc';
export async function fetchPeers() {
  let flowServiceClient = GetFlowServiceClientFromEnv();
  let req: ListPeersRequest = {};
  let peers = await flowServiceClient.listPeers(req);
  return peers.peers;
}
