'use client';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';

import {
  QRepConfig,
  QRepWriteMode,
  QRepWriteType,
} from '@/grpc_generated/flow';
import { MirrorSetting } from '../helpers/common';

interface UpsertColsProps {
  loading: boolean;
  setter: Dispatch<SetStateAction<QRepConfig>>;
  setting: MirrorSetting;
}

export default function UpsertColsDisplay({
  loading,
  setter,
  setting,
}: UpsertColsProps) {
  const [upsertColumnsText, setUpsertColumnsText] = useState<string>('');

  const handleTextChange = (value: string) => {
    setUpsertColumnsText(value);

    // Parse comma-separated values and trim whitespace
    const columnsArray = value
      .split(',')
      .map((col) => col.trim())
      .filter((col) => col.length > 0);

    setting.stateHandler(columnsArray, setter);
  };

  useEffect(() => {
    // Parse comma-separated values and update the config
    const columnsArray = upsertColumnsText
      .split(',')
      .map((col) => col.trim())
      .filter((col) => col.length > 0);

    setter((curr) => {
      let defaultMode: QRepWriteMode = {
        writeType: QRepWriteType.QREP_WRITE_MODE_APPEND,
        upsertKeyColumns: [],
      };
      let currWriteMode = (curr as QRepConfig).writeMode || defaultMode;
      currWriteMode.upsertKeyColumns = columnsArray;
      return {
        ...curr,
        writeMode: currWriteMode,
      };
    });
  }, [upsertColumnsText, setter]);

  return (
    <div style={{ marginTop: '1rem' }}>
      <input
        type='text'
        placeholder='Enter column names separated by commas (e.g., id, email, created_at)'
        value={upsertColumnsText}
        onChange={(e) => handleTextChange(e.target.value)}
        disabled={loading}
        style={{
          width: '100%',
          padding: '8px 12px',
          border: '1px solid #ccc',
          borderRadius: '4px',
          fontSize: '14px',
          backgroundColor: loading ? '#f5f5f5' : 'white',
        }}
      />
      {upsertColumnsText && (
        <div
          style={{
            marginTop: '8px',
            fontSize: '12px',
            color: '#666',
            fontStyle: 'italic',
          }}
        >
          Columns:{' '}
          {upsertColumnsText
            .split(',')
            .map((col) => col.trim())
            .filter((col) => col.length > 0)
            .join(', ')}
        </div>
      )}
    </div>
  );
}
