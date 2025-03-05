import { PostgresConfig } from '@/grpc_generated/peers';

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
];

export const blankPostgresSetting: PostgresConfig = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
};
