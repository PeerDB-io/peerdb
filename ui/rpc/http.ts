import { GetAPIToken } from '@/rpc/token';
import { ServiceError } from '@grpc/grpc-js';
import axios from 'axios';

export function GetFlowHttpAddressFromEnv() {
  return process.env.PEERDB_FLOW_SERVER_HTTP!;
}

const flowServiceHttpClient = axios.create({
  baseURL: GetFlowHttpAddressFromEnv(),
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${GetAPIToken()}`,
  },
});

export function GetFlowServiceHttpClient() {
  return flowServiceHttpClient;
}

export function ParseFlowServiceErrorMessage(error: any) {
  if (axios.isAxiosError(error)) {
    if (error.response && (error.response.data as ServiceError).code) {
      return (error.response.data as ServiceError).message;
    }
    return error.response?.data || error.message;
  }
  return error;
}
