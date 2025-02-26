import {
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
    tips: 'Specifies the IP host name or address on which postgres is to listen for TCP/IP connections from client applications. Ensure that this host has us whitelisted so we can connect to it.',
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
};
