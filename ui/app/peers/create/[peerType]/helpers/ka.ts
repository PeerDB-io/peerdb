import { KafkaConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const kaSetting: PeerSetting[] = [
  {
    label: 'Servers',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, servers: (value as string).split(',') })),
    tips: 'Brokers',
    helpfulLink:
      'https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#SeedBrokers',
  },
  {
    label: 'Username',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, username: value as string })),
  },
  {
    label: 'Password',
    type: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
  },
  {
    label: 'SASL Mechanism',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, sasl: value as string })),
    type: 'select',
    helpfulLink:
      'https://docs.redpanda.com/current/manage/security/authentication/#scram',
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    tips: 'If you are using a non-TLS connection for Kafka server, check this box.',
  },
];

export const blankKaSetting: KafkaConfig = {
  servers: [],
  username: '',
  password: '',
  sasl: 'SCRAM-SHA-512',
  disableTls: false,
};
