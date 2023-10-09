import { PeerConfig, PeerSetter } from '../types';
import { blankPostgresSetting } from './pg';
import { blankSnowflakeSetting } from './sf';

export interface PeerSetting {
  label: string;
  stateHandler: (value: string, setter: PeerSetter) => void;
  type?: string;
  optional?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number;
}

export const getBlankSetting = (dbType: string): PeerConfig => {
  switch (dbType) {
    case 'POSTGRES':
      return blankPostgresSetting;
    case 'SNOWFLAKE':
      return blankSnowflakeSetting;
    default:
      return blankPostgresSetting;
  }
};
