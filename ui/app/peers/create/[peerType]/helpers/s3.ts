import { AvroCodec, S3Config, avroCodecFromJSON } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const s3Setting: PeerSetting[] = [
  {
    label: 'Bucket URL',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, url: value as string })),
    tips: 'The URL of your existing S3/GCS bucket along with a prefix of your choice. It begins with s3://',
    helpfulLink:
      'https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html#accessing-a-bucket-using-S3-format',
    default: 's3://<bucket_name>/<prefix_name>',
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accessKeyId: value as string })),
    tips: 'The AWS access key ID associated with your account. In case of GCS, this is the HMAC access key ID.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, secretAccessKey: value as string })),
    tips: 'The AWS secret access key associated with your account. In case of GCS, this is the HMAC secret.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, region: value as string })),
    tips: 'The region where your bucket is located. For example, us-east-1. In case of GCS, this will be set to auto, which detects where your bucket it.',
  },
  {
    label: 'Endpoint',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, endpoint: value as string })),
    tips: 'The endpoint of your S3 bucket. This is optional.',
    optional: true,
  },
  {
    label: 'Role ARN',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, roleArn: value as string })),
    type: 'password',
    tips: 'If set, the role ARN will be used to assume the role before accessing the bucket.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html#identifiers-arns',
    optional: true,
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          const newCurr = { ...curr } as S3Config;
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
    label: 'TLS Host',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    tips: 'Overrides expected hostname during tls cert verification.',
    optional: true,
  },
  {
    label: 'Avro Codec',
    field: 'codec',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, codec: avroCodecFromJSON(value) })),
    type: 'select',
    placeholder: 'Select avro codec',
    options: [
      { value: 'Null', label: 'Null' },
      { value: 'Deflate', label: 'Deflate' },
      { value: 'Snappy', label: 'Snappy' },
      { value: 'ZStandard', label: 'ZStandard' },
    ],
  },
];

export const blankS3Setting: S3Config = {
  url: 's3://<bucket_name>/<prefix_name>',
  accessKeyId: undefined,
  secretAccessKey: undefined,
  roleArn: undefined,
  region: undefined,
  endpoint: '',
  rootCa: undefined,
  tlsHost: '',
  codec: AvroCodec.Null,
};
