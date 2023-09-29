import { PeerSetter } from '../types';

export const postgresSetting = [
  {
    label: 'Host',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, host: value })),
  },
  {
    label: 'Port',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, port: parseInt(value, 10) })),
    type: 'number', // type for textfield
  },
  {
    label: 'User',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, user: value })),
  },
  {
    label: 'Password',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, password: value })),
    type: 'password',
  },
  {
    label: 'Database',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, database: value })),
  },
  {
    label: 'Transaction Snapshot',
    stateHandler: (value: string, setter: PeerSetter) =>
      setter((curr) => ({ ...curr, transactionSnapshot: value })),
  },
];

export const blankPostgresSetting = {
  host: '',
  port: 5432,
  user: '',
  password: '',
  database: '',
  transactionSnapshot: '',
}
