import { S3Config } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const s3Setting: PeerSetting[] = [
  {
    label: 'Bucket URL',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, url: value })),
    tips: 'The URL of the S3/GCS bucket. It begins with s3://',
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accessKeyID: parseInt(value, 10) })),
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which postgres is listening for connections from client applications.',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, secretAccessKey: value })),
    tips: 'Specify the user that we should use to connect to this host.',
    helpfulLink: 'https://www.postgresql.org/docs/8.0/user-manag.html',
  },
  {
    label: 'Role ARN',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, roleArn: value })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
    helpfulLink: 'https://www.postgresql.org/docs/current/auth-password.html',
    optional: true,
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, region: value })),
    tips: 'Specify which database to associate with this peer.',
    helpfulLink:
      'https://www.postgresql.org/docs/current/sql-createdatabase.html',
    optional: true,
  },
  {
    label: 'Endpoint',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, endpoint: value })),
    optional: true,
    tips: '',
  },
];

export const blankS3Config: S3Config = {
  url: '',
  accessKeyId: undefined,
  secretAccessKey: undefined,
  roleArn: undefined,
  region: undefined,
  endpoint: undefined,
  metadataDb: undefined,
};
