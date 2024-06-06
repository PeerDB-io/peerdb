import { ServiceError } from '@grpc/grpc-js';
import axios from 'axios';
import bcrypt from 'bcrypt';
import 'server-only';

function hashPassword(password: string, rounds: number) {
  return bcrypt.hashSync(password, rounds);
}

export function GetAPIToken() {
  const password = process.env.PEERDB_PASSWORD!;
  const hashedPassword = hashPassword(password, 10);
  return Buffer.from(hashedPassword).toString('base64');
}

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
