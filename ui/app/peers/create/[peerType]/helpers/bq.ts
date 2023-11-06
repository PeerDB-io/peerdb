import { BigqueryConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const bigquerySetting: PeerSetting[] = [
  {
    label: 'Auth Type',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, authType: value || 'service_account' })),
    tips: 'The type of authentication to use when connecting to BigQuery.',
    default: 'service_account',
    helpfulLink:
      'https://cloud.google.com/bigquery/docs/authentication/service-account-file',
  },
  {
    label: 'Project ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, projectId: value })),
    tips: 'The ID of the Google Cloud project that owns the BigQuery dataset.',
  },
  {
    label: 'Private Key ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, privateKeyId: value })),
    tips: 'The ID of the private key used for authentication.',
  },
  {
    label: 'Private Key',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, privateKey: value })),
    type: 'file',
    tips: 'The contents of the private key used for authentication. This can be of any file extension.',
  },
  {
    label: 'Client Email',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, clientEmail: value })),
    tips: 'The email address of the service account used for authentication.',
  },
  {
    label: 'Client ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, clientId: value })),
    tips: 'The ID of the client used for authentication.',
  },
  {
    label: 'Auth URI',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, authUri: value })),
    tips: 'The URI of the authorization server used for authentication.',
  },
  {
    label: 'Token URI',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tokenUri: value })),
    tips: 'The URI of the token server used for authentication.',
  },
  {
    label: 'Auth Provider X509 Cert URL',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, authProviderX509CertUrl: value })),
    tips: 'The URL of the public x509 certificate used for authentication.',
  },
  {
    label: 'Client X509 Cert URL',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, clientX509CertUrl: value })),
    tips: 'The URL of the public x509 certificate used for authentication.',
  },
  {
    label: 'Dataset ID',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, datasetId: value })),
    tips: 'The ID of the BigQuery dataset to use.',
  },
];

export const blankBigquerySetting: BigqueryConfig = {
  authType: 'service_account',
  projectId: '',
  privateKeyId: '',
  privateKey: '',
  clientEmail: '',
  clientId: '',
  authUri: '',
  tokenUri: '',
  authProviderX509CertUrl: '',
  clientX509CertUrl: '',
  datasetId: '',
};
