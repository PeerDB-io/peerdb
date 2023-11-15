'use client';
import PeerButton from '@/components/PeerComponent';
import TimeLabel from '@/components/TimeComponent';
import { FlowConnectionConfigs } from '@/grpc_generated/flow';
import { dBTypeFromJSON } from '@/grpc_generated/peers';
import { Badge } from '@/lib/Badge';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import moment from 'moment';
import CdcGraph from './cdcGraph';

type SyncStatusRow = {
  batchId: number;
  startTime: Date;
  endTime: Date | null;
  numRows: number;
};

type props = {
  syncs: SyncStatusRow[];
  mirrorConfig: FlowConnectionConfigs | undefined;
  createdAt?: Date;
};
function CdcDetails({ syncs, createdAt, mirrorConfig }: props) {
  let lastSyncedAt = moment(syncs[0]?.startTime).fromNow();
  let rowsSynced = syncs.reduce((acc, sync) => acc + sync.numRows, 0);

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
            <div>
              <Label variant='body'>
                <Badge variant='positive' key={1}>
                  <Icon name='play_circle' />
                  Active
                </Badge>
              </Label>
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
        </div>
      </div>

      <div className='mt-10'>
        <CdcGraph syncs={syncs} />
      </div>

      <div
        style={{ display: 'flex', flexDirection: 'column', marginTop: '1rem' }}
      >
        <div className='mt-5'>
          <Label colorName='lowContrast'>Mirror Configuration</Label>
          <div
            style={{
              width: 'fit-content',
              display: 'flex',
              rowGap: '0.5rem',
              flexDirection: 'column',
            }}
          >
            <div
              className='bg-white rounded-lg p-1'
              style={{ border: '1px solid #ddd' }}
            >
              <Label variant='subheadline' colorName='lowContrast'>
                Pull Batch Size:
              </Label>
              <Label variant='body'>{mirrorConfig?.maxBatchSize}</Label>
            </div>
            <div
              className='bg-white rounded-lg p-1'
              style={{ border: '1px solid #ddd' }}
            >
              <Label variant='subheadline' colorName='lowContrast'>
                Snapshot Rows Per Partition:
              </Label>
              <Label variant='body'>
                {mirrorConfig?.snapshotNumRowsPerPartition}
              </Label>
            </div>
            <div
              className='bg-white rounded-lg p-1'
              style={{ border: '1px solid #ddd' }}
            >
              <Label variant='subheadline' colorName='lowContrast'>
                Snapshot Parallel Workers:
              </Label>
              <Label variant='body'>
                {mirrorConfig?.snapshotMaxParallelWorkers}
              </Label>
            </div>
            <div
              className='bg-white rounded-lg p-1'
              style={{ border: '1px solid #ddd' }}
            >
              <Label variant='subheadline' colorName='lowContrast'>
                Snapshot Tables In Parallel:
              </Label>
              <Label variant='body'>
                {mirrorConfig?.snapshotNumTablesInParallel}
              </Label>
            </div>
          </div>
        </div>
        <table
          style={{
            marginTop: '1rem',
            borderCollapse: 'collapse',
            width: '100%',
          }}
        >
          <thead>
            <tr style={{ borderBottom: '1px solid #ddd' }}>
              <th style={{ textAlign: 'left', padding: '0.5rem' }}>
                Source Table
              </th>
              <th style={{ textAlign: 'left', padding: '0.5rem' }}>
                Destination Table
              </th>
            </tr>
          </thead>
          <tbody>
            {tablesSynced?.map((table) => (
              <tr
                key={`${table.sourceTableIdentifier}.${table.destinationTableIdentifier}`}
                style={{ borderBottom: '1px solid #ddd' }}
              >
                <td style={{ padding: '0.5rem' }}>
                  {table.sourceTableIdentifier}
                </td>
                <td style={{ padding: '0.5rem' }}>
                  {table.destinationTableIdentifier}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function numberWithCommas(x: Number): string {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

export default CdcDetails;
