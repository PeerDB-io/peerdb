import { ClickhouseConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const clickhouseSetting: PeerSetting[] = [
  {
    label: 'Host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Specifies the IP host name or address on which ClickHouse is listening for TCP/IP connections from client applications.',
  },
  {
    label: 'Port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number', // type for textfield
    default: 9440,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which ClickHouse is listening for connections from client applications.',
  },
  {
    label: 'User',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Specify the user that we should use to connect to this host.',
  },
  {
    label: 'Password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user provided, only needed if using password authentication',
    optional: true,
  },
  {
    label: 'Database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    tips: 'Specify which database to associate with this peer.',
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    tips: 'If you are using a non-TLS connection for ClickHouse server, check this box.',
    optional: true,
  },
  {
    label: 'S3 Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, s3Path: value as string })),
    tips: `This is an S3 bucket/object URL field. This bucket will be used as our intermediate stage for CDC`,
    placeholder: 's3://<bucket-name>',
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
  {
    label: 'Endpoint',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, endpoint: value as string })),
    helpfulLink:
      'https://docs.aws.amazon.com/general/latest/gr/s3.html#s3_region',
    tips: 'An endpoint is the URL of the entry point for an AWS web service.',
    optional: true,
  },
  {
    label: 'Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          delete (curr as ClickhouseConfig)['certificate'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, certificate: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'This is only needed if the user is authenticated via certificate.',
  },
  {
    label: 'Private Key',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          delete (curr as ClickhouseConfig)['privateKey'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, privateKey: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'This is only needed if the user is authenticated via certificate.',
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          delete (curr as ClickhouseConfig)['rootCa'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'If not provided, host CA roots will be used.',
  },
];

export const blankClickHouseSetting: ClickhouseConfig = {
  host: '',
  port: 9440,
  user: '',
  password: '',
  database: '',
  s3Path: '',
  accessKeyId: '',
  secretAccessKey: '',
  region: '',
  disableTls: false,
  endpoint: undefined,
};
