import { PeerConfig, PeerSetter } from '../types';
import { blankPostgresSetting } from './pg';
import { blankSnowflakeSetting } from './sf';

export interface Setting {
  label: string;
  stateHandler: (value: string, setter: PeerSetter) => void;
  type?: string;
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
