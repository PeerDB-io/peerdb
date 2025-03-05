import { SSHConfig } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';

export type sshSetter = Dispatch<SetStateAction<SSHConfig>>;
export interface SSHSetting {
  label: string;
  stateHandler: (value: string, setter: sshSetter) => void;
  type?: string;
  optional?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number;
}

export const sshSetting: SSHSetting[] = [
  {
    label: 'Host',
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr: SSHConfig) => ({ ...curr, host: value as string })),
    tips: 'Specifies the IP host name or address of your instance.',
  },
  {
    label: 'Port',
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr) => ({ ...curr, port: parseInt(value, 10) })),
    type: 'number',
    default: 5432,
    tips: 'Specifies the TCP/IP port or local Unix domain socket file extension on which clients can connect.',
  },
  {
    label: 'User',
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr) => ({ ...curr, user: value as string })),
    tips: 'Specify the user that we should use to connect to this host.',
  },
  {
    label: 'Password',
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    type: 'password',
    optional: true,
    tips: 'Password associated with the user you provided.',
  },
  {
    label: 'SSH Private Key',
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr) => ({ ...curr, privateKey: value as string })),
    optional: true,
    type: 'file',
    tips: 'Private key for authentication in order to SSH into your machine.',
  },
  {
    label: "Host's Public Key",
    stateHandler: (value: string, setter: sshSetter) =>
      setter((curr) => ({ ...curr, hostKey: value as string })),
    optional: true,
    type: 'textarea',
    tips: 'Public key of host to mitigate MITM attacks when SSHing into your machine. It generally resides at /etc/ssh/ssh_host_[algo]_key.pub',
  },
];

export const blankSSHConfig: SSHConfig = {
  host: '',
  port: 22,
  user: '',
  password: '',
  privateKey: '',
  hostKey: '',
};
