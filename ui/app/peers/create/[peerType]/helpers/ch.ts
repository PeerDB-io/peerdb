import { ClickhouseConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';
import { blankS3Setting } from './s3';

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
    label: 'Cluster',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, cluster: value as string })),
    tips: 'Specify which cluster to associate with this peer.',
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
    label: 'Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          const newCurr = { ...curr } as ClickhouseConfig;
          delete newCurr.certificate;
          return newCurr;
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
          const newCurr = { ...curr } as ClickhouseConfig;
          delete newCurr.privateKey;
          return newCurr;
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
          const newCurr = { ...curr } as ClickhouseConfig;
          delete newCurr.rootCa;
          return newCurr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'If not provided, host CA roots will be used.',
  },
  {
    label: 'S3 Path',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          url: value as string,
        },
      })),
    tips: `This is an S3 bucket/object URL field. This bucket will be used as our intermediate stage for CDC`,
    placeholder: 's3://<bucket-name>',
    s3: true,
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          accessKeyId: value as string,
        },
      })),
    tips: 'The AWS access key ID associated with your account.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
    s3: true,
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          secretAccessKey: value as string,
        },
      })),
    tips: 'The AWS secret access key associated with the above bucket.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
    s3: true,
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          region: value as string,
        },
      })),
    tips: 'The region where your bucket is located. For example, us-east-1.',
    s3: true,
  },
  {
    label: 'Endpoint',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          endpoint: value as string,
        },
      })),
    helpfulLink:
      'https://docs.aws.amazon.com/general/latest/gr/s3.html#s3_region',
    tips: 'An endpoint is the URL of the entry point for an AWS web service.',
    optional: true,
    s3: true,
  },
  {
    label: 'S3 Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          const s3 = (curr as ClickhouseConfig).s3;
          if (s3) {
            const new3 = { ...s3 };
            delete new3.rootCa;
            return { ...curr, s3: new3 };
          }
          return curr;
        });
      } else {
        setter((curr) => ({
          ...curr,
          s3: {
            ...blankS3Setting,
            ...(curr as ClickhouseConfig).s3,
            rootCa: value as string,
          },
        }));
      }
    },
    type: 'file',
    optional: true,
    tips: 'If not provided, host CA roots will be used.',
    s3: true,
  },
  {
    label: 'S3 TLS Hostname',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        s3: {
          ...blankS3Setting,
          ...(curr as ClickhouseConfig).s3,
          tlsHost: value as string,
        },
      })),
    tips: 'Overrides expected hostname during tls cert verification.',
    optional: true,
    s3: true,
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
  tlsHost: '',
  cluster: '',
};
