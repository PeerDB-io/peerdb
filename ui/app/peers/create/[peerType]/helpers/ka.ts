import { KafkaConfig } from '@/grpc_generated/peers';
import {PeerSetting } from './common';

export const kaSetting: PeerSetting[] = [
  {
    label: 'Servers',
    stateHandler: (value, setter) => setter((curr) => ({...cur, servers: value.split(',') })),
    tips: 'Brokers',
    helpfulLink: 'https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#SeedBrokers',
  },
  {
    label: 'Username',
    stateHandler: (value, setter) => setter((curr) => ({...cur, username: value })),
  },
  {
    label: 'Password',
    type: 'password',
    stateHandler: (value, setter) => setter((curr) => ({...cur, password: value })),
  },
  {
    label: 'SASL Mechanism',
    stateHandler: (value, setter) => setter((curr) => ({...cur, sasl: value })),
    helpfulLink: 'https://docs.redpanda.com/current/manage/security/authentication/#scram',
  },
];

export const blankKaSetting: S3Config = {
  servers: [],
  username: '',
  password: '',
  sasl: 'SCRAM-SHA-512',
  disableTls: false,
};
