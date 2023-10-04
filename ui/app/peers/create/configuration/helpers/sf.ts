import { SnowflakeConfig } from '@/grpc_generated/peers';
import { Setting } from './common';

export const snowflakeSetting: Setting[] = [
  {
    label: 'Account ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accountId: value })),
  },
  {
    label: 'Username',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, username: value })),
  },
  {
    label: 'Private Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, privateKey: value })),
    type: 'file',
  },
  {
    label: 'Warehouse',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, warehouse: value })),
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value })),
  },
  {
    label: 'Role',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, role: value })),
  },
  {
    label: 'Query Timeout',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, queryTimeout: parseInt(value, 10) || 0 })),
  },
  {
    label: 'S3 Integration',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, s3Integration: value })),
  },
  {
    label: 'Password',
    stateHandler: (value, setter) => {
      if (!value.length) {
        // remove password key from state if empty
        setter((curr) => {
          delete curr['password'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, password: value }));
    },
  },
];

export const blankSnowflakeSetting: SnowflakeConfig = {
  accountId: '',
  privateKey: '',
  username: '',
  warehouse: '',
  database: '',
  role: '',
  queryTimeout: 0,
  s3Integration: '',
};
