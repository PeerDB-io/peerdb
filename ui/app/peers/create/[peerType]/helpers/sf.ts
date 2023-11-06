import { SnowflakeConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const snowflakeSetting: PeerSetting[] = [
  {
    label: 'Account ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accountId: value })),
    tips: 'This is the unique identifier for your Snowflake account. It has a URL-like format',
    helpfulLink:
      'https://docs.snowflake.com/en/user-guide/admin-account-identifier',
  },
  {
    label: 'Username',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, username: value })),
    tips: 'This is the username you use to login to your Snowflake account.',
    helpfulLink:
      'https://docs.snowflake.com/en/user-guide/admin-user-management',
  },
  {
    label: 'Private Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, privateKey: value })),
    type: 'file',
    tips: 'This can be of any file extension. If you are using an encrypted key, you must fill the below password field for decryption.',
    helpfulLink: 'https://docs.snowflake.com/en/user-guide/key-pair-auth',
  },
  {
    label: 'Warehouse',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, warehouse: value })),
    tips: 'Warehouses denote a cluster of snowflake resources.',
    helpfulLink: 'https://docs.snowflake.com/en/user-guide/warehouses',
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value })),
    tips: 'Specify which database to associate with this peer.',
    helpfulLink: 'https://docs.snowflake.com/en/sql-reference/snowflake-db',
  },
  {
    label: 'Role',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, role: value })),
    tips: 'You could use a default role, or setup a role with the required permissions.',
    helpfulLink:
      'https://docs.snowflake.com/en/user-guide/security-access-control-overview#roles',
  },
  {
    label: 'Query Timeout',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, queryTimeout: parseInt(value, 10) || 30 })),
    optional: true,
    tips: 'This is the maximum time in seconds that a query can run before being cancelled. If not specified, the default is 30 seconds',
    default: 30,
  },
  {
    label: 'S3 Integration',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, s3Integration: value })),
    optional: true,
    tips: `This is needed only if you plan to run a mirror and you wish to stage AVRO files on S3.`,
    helpfulLink:
      'https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration',
  },
  {
    label: 'Password',
    stateHandler: (value, setter) => {
      if (!value.length) {
        // remove password key from state if empty
        setter((curr) => {
          delete (curr as SnowflakeConfig)['password'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, password: value }));
    },
    type: 'password',
    optional: true,
    tips: 'This is needed only if the private key you provided is encrypted.',
    helpfulLink: 'https://docs.snowflake.com/en/user-guide/key-pair-auth',
  },
];

export const blankSnowflakeSetting: SnowflakeConfig = {
  accountId: '',
  privateKey: '',
  username: '',
  warehouse: '',
  database: '',
  role: '',
  queryTimeout: 30,
  s3Integration: '',
};
