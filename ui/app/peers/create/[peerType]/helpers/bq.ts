import { BigqueryConfig } from '@/grpc_generated/peers';

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
