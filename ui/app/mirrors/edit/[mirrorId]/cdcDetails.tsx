'use client';
import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import MirrorInfo from '@/components/MirrorInfo';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import { dBTypeFromJSON } from '@/grpc_generated/peers';
import { FlowStateChangeRequest } from '@/grpc_generated/route';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import moment from 'moment';
import Link from 'next/link';
import MirrorValues from './configValues';
import TablePairs from './tablePairs';
import { Button } from '@/lib/Button';

type props = {
  syncs: SyncStatusRow[];
  mirrorConfig: FlowConnectionConfigs;
  createdAt?: Date;
  mirrorStatus: FlowStatus;
};
function CdcDetails({ syncs, createdAt, mirrorConfig, mirrorStatus }: props) {
  let lastSyncedAt = moment(
    syncs.length > 1
      ? syncs[1]?.endTime
      : syncs.length
        ? syncs[0]?.startTime
        : new Date()
  ).fromNow();
  let rowsSynced = syncs.reduce((acc, sync) => {
    if (sync.endTime !== null) {
      return acc + sync.numRows;
    }
    return acc;
  }, 0);

  const tablesSynced = mirrorConfig.tableMappings;
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
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
              }}
            >
              <Link href={`/mirrors/errors/${mirrorConfig.flowJobName}`}>
                <Label>{formatStatus(mirrorStatus)}</Label>
              </Link>
              {statusChangeHandle(mirrorConfig, mirrorStatus)}
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
                Last Sync
              </Label>
            </div>
            <div>
              <Label variant='body'>{lastSyncedAt}</Label>
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
              <Label variant='body'>{numberWithCommas(rowsSynced)}</Label>
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

export function numberWithCommas(x: any): string {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function statusChangeHandle(
  mirrorConfig: FlowConnectionConfigs,
  mirrorStatus: FlowStatus
) {
  // hopefully there's a better way to do this cast
  if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_RUNNING]) {
    return (
      <Button
        className='IconButton'
        aria-label='Pause'
        onClick={async () => {
          const req: FlowStateChangeRequest = {
            flowJobName: mirrorConfig.flowJobName,
            sourcePeer: mirrorConfig.source,
            destinationPeer: mirrorConfig.destination,
            requestedFlowState: FlowStatus.STATUS_PAUSED,
          };
          await fetch(`/api/mirrors/state_change`, {
            method: 'POST',
            body: JSON.stringify(req),
            cache: 'no-cache'
          });
          window.location.reload();
        }}
      >
        <Icon name='pause' />
      </Button>
    );
  } else if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_PAUSED]) {
    return (
      <Button
        className='IconButton'
        aria-label='Play'
        onClick={async () => {
          const req: FlowStateChangeRequest = {
            flowJobName: mirrorConfig.flowJobName,
            sourcePeer: mirrorConfig.source,
            destinationPeer: mirrorConfig.destination,
            requestedFlowState: FlowStatus.STATUS_RUNNING,
          };
          await fetch(`/api/mirrors/state_change`, {
            method: 'POST',
            body: JSON.stringify(req),
            cache: 'no-cache'
          });
          window.location.reload();
        }}
      >
        <Icon name='play_circle' />
      </Button>
    );
  } else {
    return (
      <Button
        className='IconButton'
        aria-label='Pause (disabled)'
        disabled={true}
        style={{ opacity: '50%' }}
      >
        <Icon name='pause' />
      </Button>
    );
  }
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

export default CdcDetails;
