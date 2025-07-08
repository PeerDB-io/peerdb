import { MongoConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const mongoSetting: PeerSetting[] = [
  {
    label: 'Uri',
    field: 'uri',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, uri: value as string })),
    tips: 'SRV connection string (mongodb+srv://<srv-host>) or direct connection string (mongodb://<host1>,<host2>,<host3>).',
  },
  {
    label: 'Username',
    field: 'username',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, username: value as string })),
    tips: 'Username associated with the user you provided.',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
  },
  {
    label: 'Disable TLS?',
    field: 'disableTls',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'If you are using a non-TLS connection, check this box.',
  },
  {
    label: 'TLS Host',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    optional: true,
    tips: 'Overrides expected hostname during tls cert verification.',
  },
  {
    label: 'Root Certificate',
    field: 'rootCa',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, rootCa: value as string })),
    type: 'file',
    optional: true,
    tips: 'Root certificate for TLS connection.',
  },
];

export const blankMongoSetting: MongoConfig = {
  uri: '',
  username: '',
  password: '',
  disableTls: false,
  tlsHost: '',
  rootCa: '',
};
