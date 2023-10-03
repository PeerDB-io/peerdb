import 'server-only';

import { FlowServiceClient } from '@/grpc_generated/route';
import { credentials } from '@grpc/grpc-js';
import { promisifyClient } from './promisify';

export function GetFlowServiceClientFromEnv() {
  let address = process.env.PEERDB_FLOW_SERVER_ADDRESS!;
  console.log(`Connecting to Flow server at ${address}`);
  return promisifyClient(
    new FlowServiceClient(address, credentials.createInsecure())
  );
}
