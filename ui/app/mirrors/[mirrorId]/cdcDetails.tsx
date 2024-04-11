'use client';
import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import MirrorInfo from '@/components/MirrorInfo';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import { dBTypeFromJSON } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import MirrorValues from './configValues';
import { getCurrentIdleTimeout } from './handlers';
import { RowDataFormatter } from './rowsDisplay';
import TablePairs from './tablePairs';

type props = {
  syncs: SyncStatusRow[];
  mirrorConfig: FlowConnectionConfigs;
  createdAt?: Date;
  mirrorStatus: FlowStatus;
};
function CdcDetails({ syncs, createdAt, mirrorConfig, mirrorStatus }: props) {
  const [syncInterval, getSyncInterval] = useState<number>();

  let rowsSynced = syncs.reduce((acc, sync) => {
    if (sync.endTime !== null) {
      return acc + sync.numRows;
    }
    return acc;
  }, 0);

  const tablesSynced = mirrorConfig.tableMappings;
  useEffect(() => {
    getCurrentIdleTimeout(mirrorConfig.flowJobName).then((res) => {
      getSyncInterval(res);
    });
  }, []);
  return (
    <>
      <div className='mt-10'>
        <div className='flex flex-row'>
          <div className='basis-1/4 md:basis-1/3'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Status
              </Label>
            </div>
            <div
              style={{
                padding: '0.2rem',
                width: 'fit-content',
                borderRadius: '1rem',
                border: '1px solid rgba(0,0,0,0.1)',
              }}
            >
              <Link href={`/mirrors/errors/${mirrorConfig.flowJobName}`}>
                <Label>{formatStatus(mirrorStatus)}</Label>
              </Link>
            </div>
          </div>
          <div className='basis-1/4 md:basis-1/3'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Mirror Type
              </Label>
            </div>
            <div>
              <Label variant='body'>CDC</Label>
            </div>
          </div>
          <div className='basis-1/4 md:basis-1/3'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Source
              </Label>
            </div>
            <div>
              <PeerButton
                peerName={mirrorConfig?.source?.name ?? ''}
                peerType={dBTypeFromJSON(mirrorConfig?.source?.type)}
              />
            </div>
          </div>
          <div className='basis-1/4 md:basis-1/3'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Destination
              </Label>
            </div>
            <div>
              <PeerButton
                peerName={mirrorConfig?.destination?.name ?? ''}
                peerType={dBTypeFromJSON(mirrorConfig?.destination?.type)}
              />
            </div>
          </div>
        </div>
        <div className='flex flex-row mt-10'>
          <div className='basis-1/4'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Sync Interval
              </Label>
            </div>
            <div>
              <SyncIntervalLabel syncInterval={syncInterval} />
            </div>
          </div>
          <div className='basis-1/4'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Created At
              </Label>
            </div>
            <div>
              <TimeLabel timeVal={createdAt || ''} />
            </div>
          </div>
          <div className='basis-1/4'>
            <div>
              <Label variant='subheadline' colorName='lowContrast'>
                Rows synced
              </Label>
            </div>
            <div>
              <Label variant='body'>{RowDataFormatter(rowsSynced)}</Label>
            </div>
          </div>

          <div className='basis-1/4'>
            <MirrorInfo configs={MirrorValues(mirrorConfig)} />
          </div>
        </div>
      </div>

      <TablePairs tables={tablesSynced} />
    </>
  );
}

function formatStatus(mirrorStatus: FlowStatus) {
  const mirrorStatusLower = mirrorStatus
    .toString()
    .split('_')
    .at(-1)
    ?.toLocaleLowerCase()!;
  return (
    mirrorStatusLower.at(0)?.toLocaleUpperCase() + mirrorStatusLower.slice(1)
  );
}

const SyncIntervalLabel: React.FC<{ syncInterval?: number }> = ({
  syncInterval,
}) => {
  let formattedInterval: string;

  if (!syncInterval) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }
  if (syncInterval >= 3600) {
    const hours = Math.floor(syncInterval / 3600);
    formattedInterval = `${hours} hour${hours !== 1 ? 's' : ''}`;
  } else if (syncInterval >= 60) {
    const minutes = Math.floor(syncInterval / 60);
    formattedInterval = `${minutes} minute${minutes !== 1 ? 's' : ''}`;
  } else {
    formattedInterval = `${syncInterval} second${syncInterval !== 1 ? 's' : ''}`;
  }

  return <Label>{formattedInterval}</Label>;
};

export default CdcDetails;
