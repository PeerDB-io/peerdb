'use client';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';
import ReactSelect from 'react-select';

import SelectTheme from '@/app/styles/select';
import {
  QRepConfig,
  QRepWriteMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { Badge } from '@/lib/Badge';
import { Icon } from '@/lib/Icon';
import { MirrorSetting } from '../helpers/common';

interface UpsertColsProps {
  columns: { value: string; label: string }[];
  loading: boolean;
  setter: Dispatch<SetStateAction<QRepConfig>>;
  setting: MirrorSetting;
}
const UpsertColsDisplay = ({
  columns,
  loading,
  setter,
  setting,
}: UpsertColsProps) => {
  const [uniqueColumnsSet, setUniqueColumnsSet] = useState<Set<string>>(
    new Set<string>()
  );

  const handleUniqueColumns = (
    col: string,
    setting: MirrorSetting,
    action: 'add' | 'remove'
  ) => {
    if (action === 'add') setUniqueColumnsSet((prev) => new Set(prev).add(col));
    else if (action === 'remove') {
      setUniqueColumnsSet((prev) => {
        const newSet = new Set(prev);
        newSet.delete(col);
        return newSet;
      });
    }
    const uniqueColsArr = Array.from(uniqueColumnsSet);
    setting.stateHandler(uniqueColsArr, setter);
  };

  useEffect(() => {
    const uniqueColsArr = Array.from(uniqueColumnsSet);
    setter((curr) => {
      let defaultMode: QRepWriteMode = {
        writeType: QRepWriteType.QREP_WRITE_MODE_APPEND,
        upsertKeyColumns: [],
      };
      let currWriteMode = (curr as QRepConfig).writeMode || defaultMode;
      currWriteMode.upsertKeyColumns = uniqueColsArr as string[];
      return {
        ...curr,
        writeMode: currWriteMode,
      };
    });
  }, [uniqueColumnsSet, setter]);
  return (
    <>
      <ReactSelect
        menuPlacement='top'
        placeholder={'Select unique columns'}
        onChange={(val, action) => {
          val && handleUniqueColumns(val.value, setting, 'add');
        }}
        isLoading={loading}
        options={columns}
        theme={SelectTheme}
      />
      <div
        style={{ display: 'flex', marginTop: '0.5rem', alignItems: 'center' }}
      >
        {Array.from(uniqueColumnsSet).map((col: string) => {
          return (
            <Badge key={col} variant='normal' type='default'>
              {col}
              <button
                className='IconButton'
                onClick={() => handleUniqueColumns(col, setting, 'remove')}
                aria-label='remove col'
              >
                <Icon name='cancel' />
              </button>
            </Badge>
          );
        })}
      </div>
    </>
  );
};

export default UpsertColsDisplay;
