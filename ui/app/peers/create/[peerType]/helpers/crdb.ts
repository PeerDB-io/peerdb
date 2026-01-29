import {
  CockroachDBConfig,
} from '@/grpc_generated/peers';

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
    label: 'Require TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, requireTls: value as boolean })),
    type: 'switch',
    default: false,
    tips: 'CockroachDB Cloud requires TLS',
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
];

export const blankCockroachDBSetting: CockroachDBConfig = {
  host: '',
  port: 26257,
  user: '',
  password: '',
  database: 'defaultdb',
  requireTls: false,
  useChangefeeds: false,
};
