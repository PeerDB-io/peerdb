import { SqlServerConfig } from '@/grpc_generated/peers';

import { PeerSetting } from './common';

export const sqlServerSetting: PeerSetting[] = [
  {
    label: 'Host',
    field: 'host',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, host: value as string })),
    tips: 'Hostname or IP address of the SQL Server instance.',
  },
  {
    label: 'Port',
    field: 'port',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, port: parseInt(value as string, 10) })),
    type: 'number',
    default: 1433,
    tips: 'TCP port SQL Server is listening on.',
  },
  {
    label: 'User',
    field: 'user',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'User to connect as.',
  },
  {
    label: 'Password',
    field: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    tips: 'Password for the user.',
  },
  {
    label: 'Database',
    field: 'database',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, database: value as string })),
    tips: 'Database to connect to.',
  },
];

export const blankSqlServerSetting: SqlServerConfig = {
  host: '',
  port: 1433,
  user: '',
  password: '',
  database: '',
};
