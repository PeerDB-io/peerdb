import 'server-only';

import { FlowServiceClient } from '@/grpc_generated/route';
import { credentials } from '@grpc/grpc-js';
import { promisifyClient } from './promisify';

export function GetFlowServiceClient(address: string) {
  console.log(`Connecting to Flow server at ${address}`);
  return promisifyClient(
    new FlowServiceClient(address, credentials.createInsecure())
  );
}
