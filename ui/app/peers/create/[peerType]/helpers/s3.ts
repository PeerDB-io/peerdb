import { S3Config } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const s3Setting: PeerSetting[] = [
  {
    label: 'Bucket URL',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, url: value })),
    tips: 'The URL of your existing S3/GCS bucket along with a prefix of your choice. It begins with s3://',
    helpfulLink:
      'https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html#accessing-a-bucket-using-S3-format',
    default: 's3://<bucket_name>/<prefix_name>',
  },
  {
    label: 'Access Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, accessKeyId: value })),
    tips: 'The AWS access key ID associated with your account. In case of GCS, this is the HMAC access key ID.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Secret Access Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, secretAccessKey: value })),
    tips: 'The AWS secret access key associated with your account. In case of GCS, this is the HMAC secret.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html',
  },
  {
    label: 'Region',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, region: value })),
    tips: 'The region where your bucket is located. For example, us-east-1. In case of GCS, this will be set to auto, which detects where your bucket it.',
  },
  {
    label: 'Role ARN',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, roleArn: value })),
    type: 'password',
    tips: 'If set, the role ARN will be used to assume the role before accessing the bucket.',
    helpfulLink:
      'https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html#identifiers-arns',
    optional: true,
  },
];

export const blankMetadata = {
  host: '',
  port: 5432,
  user: 'postgres',
  password: '',
  database: 'postgres',
  transactionSnapshot: '',
};

export const blankS3Setting: S3Config = {
  url: 's3://<bucket_name>/<prefix_name>',
  accessKeyId: undefined,
  secretAccessKey: undefined,
  roleArn: undefined,
  region: undefined,
  endpoint: '',
  // For Storage peers created in UI
  // we use catalog as the metadata DB
  metadataDb: blankMetadata,
};
