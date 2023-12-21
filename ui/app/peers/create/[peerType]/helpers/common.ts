import { PeerConfig, PeerSetter } from '@/app/dto/PeersDTO';
import { blankBigquerySetting } from './bq';
import { blankPostgresSetting } from './pg';
import { blankS3Setting } from './s3';
import { blankSnowflakeSetting } from './sf';
import {blankClickhouseSetting} from './ch';

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
    case 'BIGQUERY':
      return blankBigquerySetting;
    case 'CLICKHOUSE':
      return blankClickhouseSetting;
    case 'S3':
      return blankS3Setting;
    default:
      return blankPostgresSetting;
  }
};
