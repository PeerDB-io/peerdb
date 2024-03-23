import { PubSubConfig } from '@/grpc_generated/peers';

export const blankPubSubSetting: PubSubConfig = {
  serviceAccount: {
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
  },
};
