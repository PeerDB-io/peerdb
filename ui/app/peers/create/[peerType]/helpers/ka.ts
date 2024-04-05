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
    optional: true,
  },
  {
    label: 'Password',
    type: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, password: value as string })),
    optional: true,
  },
  {
    label: 'SASL Mechanism',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, sasl: value as string })),
    type: 'select',
    placeholder: 'Select a mechanism',
    helpfulLink:
      'https://docs.redpanda.com/current/manage/security/authentication/#scram',
    options: [
      { value: 'PLAIN', label: 'PLAIN' },
      { value: 'SCRAM-SHA-256', label: 'SCRAM-SHA-256' },
      { value: 'SCRAM-SHA-512', label: 'SCRAM-SHA-512' },
    ],
  },
  {
    label: 'Partitioner',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, partitioner: value as string })),
    type: 'select',
    placeholder: 'Select a partitioner',
    helpfulLink:
      'https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Partitioner',
    options: [
      { value: 'LeastBackup', label: 'Least Backup' },
      { value: 'Manual', label: 'Manual' },
      { value: 'RoundRobin', label: 'Round Robin' },
      { value: 'StickyKey', label: 'Sticky Key' },
      { value: 'Sticky', label: 'Sticky' },
    ],
  },
  {
    label: 'Disable TLS?',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, disableTls: value as boolean })),
    type: 'switch',
    tips: 'If you are using a non-TLS connection for Kafka server, check this box.',
    optional: true,
  },
];

export const blankKafkaSetting: KafkaConfig = {
  servers: [],
  username: '',
  password: '',
  sasl: 'PLAIN',
  partitioner: '',
  disableTls: false,
};
