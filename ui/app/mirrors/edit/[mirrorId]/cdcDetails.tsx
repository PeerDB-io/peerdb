'use client';
import { SyncStatusRow } from '@/app/dto/MirrorsDTO';
import MirrorInfo from '@/components/MirrorInfo';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { FlowConnectionConfigs } from '@/grpc_generated/flow';
import { dBTypeFromJSON } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import moment from 'moment';
import Link from 'next/link';
import MirrorValues from './configValues';
import TablePairs from './tablePairs';

type props = {
  syncs: SyncStatusRow[];
  mirrorConfig: FlowConnectionConfigs | undefined;
  createdAt?: Date;
};
function CdcDetails({ syncs, createdAt, mirrorConfig }: props) {
  let lastSyncedAt = moment(syncs[0]?.endTime).fromNow();
  let rowsSynced = syncs.reduce((acc, sync) => {
    if (sync.endTime !== null) {
      return acc + sync.numRows;
    }
    return acc;
  }, 0);

  const tablesSynced = mirrorConfig?.tableMappings;
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
              }}
            >
              <Link href={`/mirrors/errors/${mirrorConfig?.flowJobName}`}>
                <Label> Active </Label>
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

export default CdcDetails;
