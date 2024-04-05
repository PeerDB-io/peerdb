import { PeerConfig, PeerSetter } from '@/app/dto/PeersDTO';
import { blankBigquerySetting } from './bq';
import { blankClickhouseSetting } from './ch';
import { blankEventHubGroupSetting } from './eh';
import { blankKafkaSetting } from './ka';
import { blankPostgresSetting } from './pg';
import { blankPubSubSetting } from './ps';
import { blankS3Setting } from './s3';
import { blankSnowflakeSetting } from './sf';

export interface PeerSetting {
  label: string;
  stateHandler: (value: string | boolean, setter: PeerSetter) => void;
  type?: string;
  optional?: boolean;
  tips?: string;
  helpfulLink?: string;
  default?: string | number;
  placeholder?: string;
  options?: { value: string; label: string }[];
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
    case 'PUBSUB':
      return blankPubSubSetting;
    case 'KAFKA':
      return blankKafkaSetting;
    case 'S3':
      return blankS3Setting;
    case 'EVENTHUBS':
      return blankEventHubGroupSetting;
    default:
      return blankPostgresSetting;
  }
};
