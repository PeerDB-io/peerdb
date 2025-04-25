import {
  AwsAuthenticationConfig,
  AwsIAMAuthConfigType,
  awsIAMAuthConfigTypeFromJSON,
  MySqlAuthType,
  mySqlAuthTypeFromJSON,
  MySqlConfig,
  MySqlFlavor,
  mySqlFlavorFromJSON,
  MySqlReplicationMechanism,
  mySqlReplicationMechanismFromJSON,
} from '@/grpc_generated/peers';

import { PeerSetting } from './common';

export const mysqlSetting: PeerSetting[] = [
  {
    label: 'Host',
    field: 'host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Specifies the IP host name or address on which mysql is to listen for TCP/IP connections from client applications. Ensure that this host has us whitelisted so we can connect to it.',
  },
  {
    label: 'Port',
    field: 'port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number',
    default: 3306,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which mysql is listening for connections from client applications.',
  },
  {
    label: 'User',
    field: 'user',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Specify the user that we should use to connect to this host.',
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
    label: 'Compression',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, compression: value ? 1 : 0 })),
    type: 'switch',
    optional: true,
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    tips: 'If you are using a non-TLS connection, check this box.',
    optional: true,
  },
  {
    label: 'Flavor',
    field: 'flavor',
    type: 'select',
    default: 'MYSQL_MYSQL',
    placeholder: 'Select a flavor',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, flavor: mySqlFlavorFromJSON(value) })),
    options: [
      { value: 'MYSQL_MYSQL', label: 'MySQL' },
      { value: 'MYSQL_MARIA', label: 'MariaDB' },
    ],
  },
  {
    label: 'Replication',
    field: 'replicationMechanism',
    type: 'select',
    default: 'MYSQL_AUTO',
    placeholder: 'Select a replication mechanism',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...curr,
        replicationMechanism: mySqlReplicationMechanismFromJSON(value),
      })),
    options: [
      { value: 'MYSQL_AUTO', label: 'Auto' },
      { value: 'MYSQL_GTID', label: 'GTID' },
      { value: 'MYSQL_FILEPOS', label: 'FilePos' },
    ],
  },
  {
    label: 'Root Certificate',
    stateHandler: (value, setter) => {
      if (!value) {
        // remove key from state if empty
        setter((curr) => {
          delete (curr as MySqlConfig)['rootCa'];
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
    default: 'MYSQL_PASSWORD',
    stateHandler: (value, setter) =>
      setter((curr) => ({
        ...(curr as MySqlConfig),
        authType: mySqlAuthTypeFromJSON(value),
      })),
    type: 'select',
    placeholder: 'Select authentication mechanism',
    options: [
      { value: 'MYSQL_PASSWORD', label: 'Password' },
      { value: 'MYSQL_IAM_AUTH', label: 'AWS IAM Auth' },
    ],
    tips: 'AWS IAM Auth is supported for all Mirror Types for MySQL',
  },
  {
    label: 'AWS IAM Auth Mechanism',
    field: 'awsAuth.authType',
    default: 'IAM_AUTH_AUTOMATIC',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let cfg = curr as MySqlConfig;
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
        let mysqlConfig = curr as MySqlConfig;
        mysqlConfig.awsAuth!.role!.assumeRoleArn = value as string;
        return { ...mysqlConfig };
      }),
    tips: 'AWS IAM Role to assume.',
  },
  {
    label: 'Chained IAM Role to Assume',
    field: 'awsAuth.role.chainedIamRole',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let mysqlConfig = curr as MySqlConfig;
        mysqlConfig.awsAuth!.role!.chainedRoleArn = value as string;
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
        let mysqlConfig = curr as MySqlConfig;
        mysqlConfig.awsAuth!.staticCredentials!.accessKeyId = value as string;
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
        let mysqlConfig = curr as MySqlConfig;
        mysqlConfig.awsAuth!.staticCredentials!.secretAccessKey =
          value as string;
        return { ...curr };
      }),
    type: 'password',
    tips: 'AWS Secret Access Key',
  },
];

export const blankMySqlSetting: MySqlConfig = {
  host: '',
  port: 3306,
  user: '',
  password: '',
  database: '',
  setup: [],
  compression: 0,
  disableTls: false,
  flavor: MySqlFlavor.MYSQL_MYSQL,
  replicationMechanism: MySqlReplicationMechanism.MYSQL_AUTO,
  authType: MySqlAuthType.MYSQL_PASSWORD,
  awsAuth: {
    region: '',
    authType: AwsIAMAuthConfigType.IAM_AUTH_AUTOMATIC,
  },
  tlsHost: '',
};
