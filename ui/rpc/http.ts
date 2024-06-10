import { GetAuthorizationHeader } from '@/rpc/token';

export function GetFlowHttpAddressFromEnv() {
  return process.env.PEERDB_FLOW_SERVER_HTTP!;
}

function handleResponse(res: Response) {
  if (!res.ok) {
    throw res;
  }
  return res.json();
}

class Client {
  baseUrl: string;
  headers: Headers;

  constructor(baseUrl: string, headers: Headers) {
    this.baseUrl = baseUrl;
    this.headers = headers;
  }

  raw(path: string, options?: { [key: string]: any }) {
    return fetch(this.baseUrl + path, { headers: this.headers, ...options });
  }

  async get(path: string, options?: { [key: string]: any }) {
    const res = await this.raw(path, options);
    return handleResponse(res);
  }

  async post(path: string, options?: { [key: string]: any }) {
    const res = await this.raw(path, {
      method: 'POST',
      ...options,
    });
    return handleResponse(res);
  }
}

const flowServiceHeaders = new Headers({
  'Content-Type': 'application/json',
});
if (GetAuthorizationHeader()) {
  flowServiceHeaders.set('Authorization', GetAuthorizationHeader());
}
const flowServiceHttpClient = new Client(
  GetFlowHttpAddressFromEnv(),
  flowServiceHeaders
);

export function GetFlowServiceHttpClient() {
  return flowServiceHttpClient;
}

export async function ParseFlowServiceErrorMessage(error: any) {
  if (error instanceof Response) {
    const text = await error.text();
    return `${error.status} ${error.statusText} ${text}`;
  } else if (error instanceof Error) {
    return error.message;
  } else {
    return error;
  }
}
