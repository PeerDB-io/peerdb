import { MongoConfig } from '@/grpc_generated/peers';
import { PeerSetting } from './common';

export const mongoSetting: PeerSetting[] = [
  {
    label: 'Uri',
    field: 'uri',
    stateHandler: (value, setter) =>
      setter((curr) => ({ ...curr, uri: value as string })),
    tips: 'MongoDB connection string',
  },
];

export const blankMongoSetting: MongoConfig = {
  uri: '',
};
