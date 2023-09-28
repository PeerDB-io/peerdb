import { CreatePeerRequest, ValidatePeerRequest } from '@/grpc_generated/route';
import { GetFlowServiceClient } from '@/rpc/rpc';

export async function validatePeer(req: ValidatePeerRequest) {
    let flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
    let flowServiceClient = GetFlowServiceClient(flowServiceAddress);
}

export async function createPeer(req: CreatePeerRequest) {
    let flowServiceAddress = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
    let flowServiceClient = GetFlowServiceClient(flowServiceAddress);
}