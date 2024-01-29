import { ClickhouseConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const clickhouseSetting: PeerSetting[] = [
  {
    label: 'Host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value })),
    tips: 'Specifies the IP host name or address on which postgres is to listen for TCP/IP connections from client applications. Ensure that this host has us whitelisted so we can connect to it.',
  },
  {
    label: 'Port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value, 10) })),
    type: 'number', // type for textfield
    default: 5432,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which postgres is listening for connections from client applications.',
  },
  {
    label: 'User',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value })),
    tips: 'Specify the user that we should use to connect to this host.',
    helpfulLink: 'https://www.postgresql.org/docs/8.0/user-manag.html',
  },
  {
    label: 'Password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
    helpfulLink: 'https://www.postgresql.org/docs/current/auth-password.html',
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value })),
    tips: 'Specify which database to associate with this peer.',
    helpfulLink:
      'https://www.postgresql.org/docs/current/sql-createdatabase.html',
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
];

export const blankClickhouseSetting: ClickhouseConfig = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
  s3Integration: '',
  metadataDb: undefined,
};
