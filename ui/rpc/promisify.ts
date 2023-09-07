import type {
  CallOptions,
  Client,
  ClientUnaryCall,
  Metadata,
  ServiceError,
} from '@grpc/grpc-js';

type OriginalCall<T, U> = (
  request: T,
  metadata: Metadata,
  options: Partial<CallOptions>,
  callback: (err: ServiceError | null, res?: U) => void
) => ClientUnaryCall;

type PromisifiedCall<T, U> = (
  request: T,
  metadata?: Metadata,
  options?: Partial<CallOptions>
) => Promise<U>;

export type PromisifiedClient<C> = { $: C } & {
  [prop in Exclude<keyof C, keyof Client>]: C[prop] extends OriginalCall<
    infer T,
    infer U
  >
    ? PromisifiedCall<T, U>
    : never;
};

export function promisifyClient<C extends Client>(client: C) {
  return new Proxy(client, {
    get: (target, descriptor) => {
      const key = descriptor as keyof PromisifiedClient<C>;

      if (key === '$') return target;

      const func = target[key];
      if (typeof func === 'function')
        return (...args: unknown[]) =>
          new Promise((resolve, reject) =>
            func.call(
              target,
              ...[
                ...args,
                (err: unknown, res: unknown) =>
                  err ? reject(err) : resolve(res),
              ]
            )
          );
    },
  }) as unknown as PromisifiedClient<C>;
}
