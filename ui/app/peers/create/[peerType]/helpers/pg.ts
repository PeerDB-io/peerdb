import {
  AwsAuthenticationConfig,
  AwsIAMAuthConfigType,
  awsIAMAuthConfigTypeFromJSON,
  PostgresAuthType,
  postgresAuthTypeFromJSON,
  PostgresConfig,
} from '@/grpc_generated/peers';

import { PeerSetting } from './common';

export const postgresSetting: PeerSetting[] = [
  {
    label: 'Host',
    field: 'host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Specifies the IP host name or address on which postgres is to listen for TCP/IP connections from client applications. Ensure that this host has us whitelisted so we can connect to it.',
  },
  {
    label: 'Port',
    field: 'port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number',
    default: 5432,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which postgres is listening for connections from client applications.',
  },
  {
    label: 'User',
    field: 'user',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Specify the user that we should use to connect to this host.',
    helpfulLink: 'https://www.postgresql.org/docs/8.0/user-manag.html',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password associated with the user you provided.',
    helpfulLink: 'https://www.postgresql.org/docs/current/auth-password.html',
  },
  {
    label: 'Database',
    field: 'database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    tips: 'Specify which database to associate with this peer.',
    helpfulLink:
      'https://www.postgresql.org/docs/current/sql-createdatabase.html',
  },
  {
    label: 'Require TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, requireTls: value as boolean })),
    type: 'switch',
    tips: 'Use sslmode=require instead of sslmode=prefer',
    optional: true,
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          delete (curr as PostgresConfig)['rootCa'];
          return curr;
        });
      } else setter((curr) => ({ ...curr, rootCa: value as string }));
    },
    type: 'file',
    optional: true,
    tips: 'If not provided, host CA roots will be used.',
  },
  {
    label: 'TLS Hostname',
    field: 'tlsHost',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, tlsHost: value as string })),
    tips: 'Overrides expected hostname during tls cert verification.',
    optional: true,
  },
  {
    label: 'Authentication type',
    field: 'authType',
    default: 'POSTGRES_PASSWORD',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...(curr as PostgresConfig),
        authType: postgresAuthTypeFromJSON(value),
      })),
    type: 'select',
    placeholder: 'Select authentication mechanism',
    options: [
      { value: 'POSTGRES_PASSWORD', label: 'Password' },
      { value: 'POSTGRES_IAM_AUTH', label: 'AWS IAM Auth' },
    ],
    tips: 'AWS IAM Auth is supported for Initial-Load-Only Mirrors. It is NOT SUPPORTED for CDC',
  },
  {
    label: 'AWS IAM Auth Mechanism',
    field: 'awsAuth.authType',
    default: 'IAM_AUTH_AUTOMATIC',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let cfg = curr as PostgresConfig;
        let awsAuth: AwsAuthenticationConfig = {
          region: '',
          authType: awsIAMAuthConfigTypeFromJSON(value),
        };
        switch (awsAuth.authType) {
          case AwsIAMAuthConfigType.IAM_AUTH_STATIC_CREDENTIALS:
            awsAuth = {
              ...awsAuth,
              staticCredentials: {
                accessKeyId: '',
                secretAccessKey: '',
              },
            };
            break;
          case AwsIAMAuthConfigType.IAM_AUTH_ASSUME_ROLE:
            awsAuth = {
              ...awsAuth,
              role: {
                assumeRoleArn: '',
              },
            };
            break;
        }
        const newCfg = {
          ...cfg,
          awsAuth: awsAuth,
        };
        return newCfg;
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
        let pgConfig = curr as PostgresConfig;
        pgConfig.awsAuth!.role!.assumeRoleArn = value as string;
        return { ...pgConfig };
      }),
    tips: 'AWS IAM Role to assume.',
  },
  {
    label: 'Chained IAM Role to Assume',
    field: 'awsAuth.role.chainedIamRole',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let pgConfig = curr as PostgresConfig;
        pgConfig.awsAuth!.role!.chainedRoleArn = value as string;
        return { ...curr };
      }),
    tips: 'Additional Chained AWS IAM Role to assume after assuming the first role.',
    optional: true,
  },
  {
    label: 'IAM Auth: AWS Access Key ID',
    field: 'awsAuth.staticCredentials.accessKeyId',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let pgConfig = curr as PostgresConfig;
        pgConfig.awsAuth!.staticCredentials!.accessKeyId = value as string;
        return { ...curr };
      }),
    type: 'password',
    tips: 'AWS Access key ID',
  },
  {
    label: 'IAM Auth: AWS Secret Access Key',
    field: 'awsAuth.staticCredentials.secretAccessKey',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let pgConfig = curr as PostgresConfig;
        pgConfig.awsAuth!.staticCredentials!.secretAccessKey = value as string;
        return { ...curr };
      }),
    type: 'password',
    tips: 'AWS Secret Access Key',
  },
];

export const blankPostgresSetting: PostgresConfig = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
  requireTls: false,
  authType: PostgresAuthType.POSTGRES_PASSWORD,
  awsAuth: {
    region: '',
    authType: AwsIAMAuthConfigType.IAM_AUTH_AUTOMATIC,
  },
  tlsHost: '',
};
