import {
  ElasticsearchAuthType,
  elasticsearchAuthTypeFromJSON,
  ElasticsearchConfig,
} from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const esSetting: PeerSetting[] = [
  {
    label: 'Addresses',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, addresses: (value as string).split(',') })),
  },
  {
    label: 'Authentication type',
    stateHandler: (value, setter) =>
      setter((curr) => {
        let esConfig = curr as ElasticsearchConfig;
        return {
          ...esConfig,
          authType: elasticsearchAuthTypeFromJSON(value),
        };
      }),
    type: 'select',
    placeholder: 'Select a mechanism',
    options: [
      { value: 'NONE', label: 'None' },
      { value: 'BASIC', label: 'Basic' },
      { value: 'APIKEY', label: 'API Key' },
    ],
  },
  // remaining fields are optional but displayed conditionally so not optional style wise
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
    label: 'API Key',
    type: 'password',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, apiKey: value as string })),
  },
];

export const blankElasticsearchSetting: ElasticsearchConfig = {
  addresses: [],
  authType: ElasticsearchAuthType.UNKNOWN,
  username: '',
  password: '',
  apiKey: '',
};
