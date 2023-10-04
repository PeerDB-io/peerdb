import { PostgresConfig } from '@/grpc_generated/peers';
import { Setting } from './common';

export const postgresSetting: Setting[] = [
  {
    label: 'Host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value })),
  },
  {
    label: 'Port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value, 10) })),
    type: 'number', // type for textfield
  },
  {
    label: 'User',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value })),
  },
  {
    label: 'Password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value })),
    type: 'password',
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value })),
  },
  {
    label: 'Transaction Snapshot',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, transactionSnapshot: value })),
  },
];

export const blankPostgresSetting: PostgresConfig = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
  transactionSnapshot: '',
};
