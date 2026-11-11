'use client';
import { FlowStatus } from '@/grpc_generated/flow';
import { CDCMirrorStatus } from '@/grpc_generated/route';
import MirrorInfo from '@/components/MirrorInfo';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useEffect, useState } from 'react';
import MirrorValues from './configValues';
import { getCurrentIdleTimeout } from './handlers';
import { RowDataFormatter } from './rowsDisplay';
import TablePairs from './tablePairs';

type props = {
  mirrorConfig: CDCMirrorStatus;
  createdAt?: Date;
  mirrorStatus: FlowStatus;
};

export function CdcFacts({ mirrorConfig }: { mirrorConfig: CDCMirrorStatus }) {
  const [syncInterval, setSyncInterval] = useState<number>();

  useEffect(() => {
    getCurrentIdleTimeout(mirrorConfig.config?.flowJobName ?? '').then((res) =>
      setSyncInterval(res)
    );
  }, [mirrorConfig.config?.flowJobName]);

  return (
    <div style={{ marginTop: '14px' }}>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, minmax(0, 1fr))',
          gap: '12px',
        }}
      >
        <div>
          <div style={{ fontSize: 12, fontWeight: 500, opacity: 0.7 }}>Rows synced</div>
          <div style={{ fontSize: 16, fontWeight: 500 }}>
            {RowDataFormatter(mirrorConfig.rowsSynced)}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 12, fontWeight: 500, opacity: 0.7 }}>Sync interval</div>
          <div style={{ fontSize: 16, fontWeight: 500 }}>
            <SyncIntervalLabel syncInterval={syncInterval} />
          </div>
        </div>
        <div>
          <div style={{ fontSize: 12, fontWeight: 500, opacity: 0.7 }}>Source</div>
          <div style={{ fontSize: 16, fontWeight: 500 }}>
            {mirrorConfig.config?.sourceName}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 12, fontWeight: 500, opacity: 0.7 }}>Destination</div>
          <div style={{ fontSize: 16, fontWeight: 500 }}>
            {mirrorConfig.config?.destinationName}
          </div>
        </div>
      </div>
      <div style={{ marginTop: '12px' }}>
        <MirrorInfo configs={MirrorValues(mirrorConfig.config)} />
      </div>
    </div>
  );
}

export default function CdcDetails({ mirrorConfig }: props) {
  return (
    <TablePairs
      tables={mirrorConfig.config?.tableMappings}
      sourcePeerName={mirrorConfig.config?.sourceName ?? ''}
    />
  );
}

function SyncIntervalLabel({ syncInterval }: { syncInterval?: number }) {
  let formattedInterval: string;

  if (!syncInterval) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  } else if (syncInterval >= 3600) {
    const hours = Math.floor(syncInterval / 3600);
    formattedInterval = `${hours} hour${hours !== 1 ? 's' : ''}`;
  } else if (syncInterval >= 60) {
    const minutes = Math.floor(syncInterval / 60);
    formattedInterval = `${minutes} minute${minutes !== 1 ? 's' : ''}`;
  } else {
    formattedInterval = `${syncInterval} second${syncInterval !== 1 ? 's' : ''}`;
  }

  return <Label>{formattedInterval}</Label>;
}
