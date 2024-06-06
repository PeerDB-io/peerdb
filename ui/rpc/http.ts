import { GetAPIToken } from '@/rpc/token';

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
  headers: { [key: string]: any };

  constructor(baseUrl: string, headers: { [key: string]: any }) {
    this.baseUrl = baseUrl;
    this.headers = headers;
  }

  raw(path: string, headers?: { [key: string]: any }) {
    return fetch(this.baseUrl + path, { ...this.headers, ...headers });
  }

  get(path: string, headers?: { [key: string]: any }) {
    return this.raw(path, headers).then(handleResponse);
  }

  post(path: string, headers?: { [key: string]: any }) {
    return this.raw(path, {
      method: 'POST',
      ...headers,
    }).then(handleResponse);
  }
}

const flowServiceHttpClient = new Client(GetFlowHttpAddressFromEnv(), {
  'Content-Type': 'application/json',
  Authorization: `Bearer ${GetAPIToken()}`,
});

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
