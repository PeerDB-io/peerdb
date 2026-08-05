import { CockroachDBConfig } from '@/grpc_generated/peers';

import { PeerSetting } from './common';

export const cockroachdbSetting: PeerSetting[] = [
  {
    label: 'Host',
    field: 'host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'CockroachDB cluster hostname',
  },
  {
    label: 'Port',
    field: 'port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number',
    default: 26257,
    tips: 'CockroachDB port (default: 26257)',
  },
  {
    label: 'User',
    field: 'user',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Database user',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    optional: true,
    tips: 'User password (optional for some configurations)',
  },
  {
    label: 'Database',
    field: 'database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    default: 'defaultdb',
    tips: 'Database name (default: defaultdb)',
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'TLS is on by default; only disable it for insecure-mode clusters. CockroachDB Cloud requires TLS.',
  },
  {
    label: 'Skip Certificate Verification?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, skipCertVerification: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'Skip TLS certificate verification (insecure, use with caution).',
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        setter((curr) => {
          const newCurr = { ...curr } as CockroachDBConfig;
          delete newCurr.rootCa;
          return newCurr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'Root CA certificate for TLS connections',
  },
  {
    label: 'TLS Hostname',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    tips: 'Overrides expected hostname during TLS cert verification',
    optional: true,
  },
  {
    label: 'Client Certificate',
    field: 'clientTls.certificate',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const crdb = curr as CockroachDBConfig;
        const certificate = value as string;
        if (!certificate && !crdb.clientTls?.privateKey) {
          const newCurr = { ...crdb };
          delete newCurr.clientTls;
          return newCurr;
        }
        return {
          ...crdb,
          clientTls: {
            certificate,
            privateKey: crdb.clientTls?.privateKey ?? '',
          },
        };
      }),
    type: 'file',
    optional: true,
    tips: 'Client certificate (PEM) presented to the server for mutual TLS. Requires TLS to be enabled, and must be paired with the client private key.',
  },
  {
    label: 'Client Private Key',
    field: 'clientTls.privateKey',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const crdb = curr as CockroachDBConfig;
        const privateKey = value as string;
        if (!privateKey && !crdb.clientTls?.certificate) {
          const newCurr = { ...crdb };
          delete newCurr.clientTls;
          return newCurr;
        }
        return {
          ...crdb,
          clientTls: {
            certificate: crdb.clientTls?.certificate ?? '',
            privateKey,
          },
        };
      }),
    type: 'file',
    optional: true,
    tips: 'Private key (PEM) for the client certificate. Requires TLS to be enabled, and must be paired with the client certificate.',
  },
];

export const blankCockroachDBSetting: CockroachDBConfig = {
  host: '',
  port: 26257,
  user: '',
  password: '',
  database: 'defaultdb',
  tlsHost: '',
  disableTls: false,
  skipCertVerification: false,
};
