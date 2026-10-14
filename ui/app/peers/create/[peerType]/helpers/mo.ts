import {
  AwsAuthenticationConfig,
  AwsIAMAuthConfigType,
  awsIAMAuthConfigTypeFromJSON,
  MongoAuthType,
  mongoAuthTypeFromJSON,
  MongoConfig,
} from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const mongoSetting: PeerSetting[] = [
  {
    label: 'Uri',
    field: 'uri',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, uri: value as string })),
    tips: 'SRV connection string (mongodb+srv://<srv-host>) or direct connection string (mongodb://<host1>,<host2>,<host3>).',
  },
  {
    label: 'Username',
    field: 'username',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, username: value as string })),
    tips: 'Username associated with the user you provided.',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
  },
  {
    label: 'Authentication type',
    field: 'authType',
    default: 'MONGO_PASSWORD',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...(curr as MongoConfig),
        authType: mongoAuthTypeFromJSON(value),
      })),
    type: 'select',
    placeholder: 'Select authentication mechanism',
    options: [
      { value: 'MONGO_PASSWORD', label: 'Password' },
      { value: 'MONGO_IAM_AUTH', label: 'AWS IAM Auth' },
    ],
    tips: 'AWS IAM Auth logs in with MONGODB-AWS, as supported by Amazon DocumentDB. Username and password are ignored.',
  },
  {
    label: 'AWS IAM Auth Mechanism',
    field: 'awsAuth.authType',
    default: 'IAM_AUTH_AUTOMATIC',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let awsAuth: AwsAuthenticationConfig = {
          region: '',
          authType: awsIAMAuthConfigTypeFromJSON(value),
        };
        switch (awsAuth.authType) {
          case AwsIAMAuthConfigType.IAM_AUTH_STATIC_CREDENTIALS:
            awsAuth = {
              ...awsAuth,
              staticCredentials: { accessKeyId: '', secretAccessKey: '' },
            };
            break;
          case AwsIAMAuthConfigType.IAM_AUTH_ASSUME_ROLE:
            awsAuth = { ...awsAuth, role: { assumeRoleArn: '' } };
            break;
        }
        return { ...(curr as MongoConfig), awsAuth };
      }),
    type: 'select',
    placeholder: 'Select authentication mechanism',
    options: [
      { value: 'IAM_AUTH_AUTOMATIC', label: 'Automatic' },
      { value: 'IAM_AUTH_STATIC_CREDENTIALS', label: 'Static Credentials' },
      { value: 'IAM_AUTH_ASSUME_ROLE', label: 'IAM Role Chaining' },
    ],
    tips: 'Automatic uses the default AWS credentials provider chain. Static Credentials uses the static credentials you provide. IAM Role Chaining uses the role you provide to assume a different role (along with a chained role if provided).',
  },
  {
    label: 'IAM Role to Assume',
    field: 'awsAuth.role.assumeIamRole',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const cfg = curr as MongoConfig;
        return {
          ...cfg,
          awsAuth: {
            ...cfg.awsAuth!,
            role: { ...cfg.awsAuth!.role!, assumeRoleArn: value as string },
          },
        };
      }),
    tips: 'AWS IAM Role to assume.',
  },
  {
    label: 'Chained IAM Role to Assume',
    field: 'awsAuth.role.chainedIamRole',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const cfg = curr as MongoConfig;
        return {
          ...cfg,
          awsAuth: {
            ...cfg.awsAuth!,
            role: { ...cfg.awsAuth!.role!, chainedRoleArn: value as string },
          },
        };
      }),
    tips: 'Additional Chained AWS IAM Role to assume after assuming the first role. This role must be registered as a user in the $external database.',
    optional: true,
  },
  {
    label: 'IAM Auth: AWS Access Key ID',
    field: 'awsAuth.staticCredentials.accessKeyId',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const cfg = curr as MongoConfig;
        return {
          ...cfg,
          awsAuth: {
            ...cfg.awsAuth!,
            staticCredentials: {
              ...cfg.awsAuth!.staticCredentials!,
              accessKeyId: value as string,
            },
          },
        };
      }),
    type: 'password',
    tips: 'AWS Access key ID',
  },
  {
    label: 'IAM Auth: AWS Secret Access Key',
    field: 'awsAuth.staticCredentials.secretAccessKey',
    stateHandler: (value, setter) =>
      setter((curr) => {
        const cfg = curr as MongoConfig;
        return {
          ...cfg,
          awsAuth: {
            ...cfg.awsAuth!,
            staticCredentials: {
              ...cfg.awsAuth!.staticCredentials!,
              secretAccessKey: value as string,
            },
          },
        };
      }),
    type: 'password',
    tips: 'AWS Secret Access Key',
  },
  {
    label: 'Disable TLS?',
    field: 'disableTls',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'If you are using a non-TLS connection, check this box.',
  },
  {
    label: 'Skip Certificate Verification?',
    field: 'skipCertVerification',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, skipCertVerification: value as boolean })),
    type: 'switch',
    optional: true,
    tips: 'Skip TLS certificate verification (insecure, use with caution).',
  },
  {
    label: 'TLS Host',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    optional: true,
    tips: 'Overrides expected hostname during tls cert verification.',
  },
  {
    label: 'Root Certificate',
    field: 'rootCa',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          const newCurr = { ...curr } as MongoConfig;
          delete newCurr.rootCa;
          return newCurr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'Root certificate for TLS connection.',
  },
];

export const blankMongoSetting: MongoConfig = {
  uri: '',
  username: '',
  password: '',
  disableTls: false,
  skipCertVerification: false,
  tlsHost: '',
  readPreference: 0,
  authType: MongoAuthType.MONGO_PASSWORD,
  awsAuth: {
    region: '',
    authType: AwsIAMAuthConfigType.IAM_AUTH_AUTOMATIC,
  },
};
