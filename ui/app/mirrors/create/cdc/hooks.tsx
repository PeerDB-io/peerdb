import { MirrorSetter } from '@/app/dto/MirrorsDTO';
import { DBType } from '@/grpc_generated/peers';
import { IsClickhousePeer, IsQueuePeer } from '../handlers';
import { AdvancedSettingType, MirrorSetting } from '../helpers/common';

export const AdjustAdvancedSetting = (
  setting: MirrorSetting,
  destinationType: DBType,
  setter: MirrorSetter
) => {
  if (
    IsQueuePeer(destinationType) &&
    setting.advanced === AdvancedSettingType.QUEUE &&
    setting.label === 'Sync Interval (Seconds)'
  ) {
    setting.stateHandler(600, setter);
    return { ...setting, default: 600 };
  }
  if (
    IsClickhousePeer(destinationType) &&
    (setting.label === 'Pull Batch Size' ||
      setting.label === 'Snapshot Number of Rows Per Partition')
  ) {
    setting.stateHandler(100000, setter);
    return { ...setting, default: 100000 };
  }
  if (setting.advanced === AdvancedSettingType.ALL) {
    return setting;
  }
};
