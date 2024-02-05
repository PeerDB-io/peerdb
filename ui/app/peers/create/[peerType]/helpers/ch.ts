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
    label: 'S3 Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, s3Path: value })),
    tips: `This is an S3 bucket/object URL field. This bucket will be used as our intermediate stage for CDC`,
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accessKeyId: value })),
    tips: 'The AWS access key ID associated with your account.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, secretAccessKey: value })),
    tips: 'The AWS secret access key associated with the above bucket.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, region: value })),
    tips: 'The region where your bucket is located. For example, us-east-1.',
  },
];

export const blankClickhouseSetting: ClickhouseConfig = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
  s3Path: '',
  accessKeyId: '',
  secretAccessKey: '',
  region: '',
};
