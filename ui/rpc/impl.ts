import grpc from '@grpc/grpc-js';

type RpcImpl = (
  service: string,
  method: string,
  data: Uint8Array
) => Promise<Uint8Array>;

type UnaryCallback<T> = (error: grpc.ServiceError | null, value?: T) => void;

const conn = new grpc.Client(
  'localhost:50051',
  grpc.credentials.createInsecure()
);

const sendRequest: RpcImpl = (service, method, data) => {
  const path = `/${service}/${method}`;

  return new Promise((resolve, reject) => {
    const resultCallback: UnaryCallback<any> = (err, res) => {
      if (err) {
        return reject(err);
      }
      resolve(res);
    };

    function passThrough(argument: any) {
      return argument;
    }

    // Using passThrough as the serialize and deserialize functions
    conn.makeUnaryRequest(path, passThrough, passThrough, data, resultCallback);
  });
};

const rpc: Rpc = { request: sendRequest };
