'use client';
import { FormatStatus } from '@/app/utils/flowstatus';
import MirrorInfo from '@/components/MirrorInfo';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { FlowStatus } from '@/grpc_generated/flow';
import { dBTypeFromJSON } from '@/grpc_generated/peers';
import { CDCMirrorStatus } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
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

export default function CdcDetails({
  createdAt,
  mirrorConfig,
  mirrorStatus,
}: props) {
  const [syncInterval, setSyncInterval] = useState<number>();

  const tablesSynced = mirrorConfig.config?.tableMappings;
  useEffect(() => {
    getCurrentIdleTimeout(mirrorConfig.config?.flowJobName ?? '').then((res) =>
      setSyncInterval(res)
    );
  }, [mirrorConfig.config?.flowJobName]);
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
              <Link
                href={`/mirrors/errors/${mirrorConfig.config?.flowJobName}`}
              >
                <Label>{FormatStatus(mirrorStatus)}</Label>
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
                peerName={mirrorConfig.config?.sourceName ?? ''}
                peerType={dBTypeFromJSON(mirrorConfig.sourceType)}
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
                peerName={mirrorConfig.config?.destinationName ?? ''}
                peerType={dBTypeFromJSON(mirrorConfig.destinationType)}
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
              <Label variant='body'>
                {RowDataFormatter(mirrorConfig.rowsSynced)}
              </Label>
            </div>
          </div>

          <div className='basis-1/4'>
            <MirrorInfo configs={MirrorValues(mirrorConfig.config)} />
          </div>
        </div>
      </div>

      <TablePairs tables={tablesSynced} />
    </>
  );
}

const SyncIntervalLabel: React.FC<{ syncInterval?: number }> = ({
  syncInterval,
}) => {
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
};
