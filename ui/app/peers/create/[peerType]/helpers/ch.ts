import { ClickhouseConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const clickhouseSetting: PeerSetting[] = [
  {
    label: 'Host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Specifies the IP host name or address on which Clickhouse is listening for TCP/IP connections from client applications.',
  },
  {
    label: 'Port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number', // type for textfield
    default: 9000,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which Clickhouse is listening for connections from client applications.',
  },
  {
    label: 'User',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Specify the user that we should use to connect to this host.',
    helpfulLink: 'https://www.postgresql.org/docs/8.0/user-manag.html',
  },
  {
    label: 'Password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
    helpfulLink: 'https://www.postgresql.org/docs/current/auth-password.html',
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    tips: 'Specify which database to associate with this peer.',
    helpfulLink:
      'https://www.postgresql.org/docs/current/sql-createdatabase.html',
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    tips: 'If you are using a non-TLS connection for Clickhouse server, check this box.',
  },
  {
    label: 'S3 Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, s3Path: value as string })),
    tips: `This is an S3 bucket/object URL field. This bucket will be used as our intermediate stage for CDC`,
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accessKeyId: value as string })),
    tips: 'The AWS access key ID associated with your account.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, secretAccessKey: value as string })),
    tips: 'The AWS secret access key associated with the above bucket.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, region: value as string })),
    tips: 'The region where your bucket is located. For example, us-east-1.',
  },
];

export const blankClickhouseSetting: ClickhouseConfig = {
  host: '',
  port: 9000,
  user: '',
  password: '',
  database: '',
  s3Path: '',
  accessKeyId: '',
  secretAccessKey: '',
  region: '',
  disableTls: false,
};
