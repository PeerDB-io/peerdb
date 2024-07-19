'use client';

import NewButton from '@/components/NewButton';
import { ListMirrorsResponse } from '@/grpc_generated/route';
import { Header } from '@/lib/Header';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../utils/swr';
import { CDCFlows, QRepFlows } from './tables';

export default function Mirrors() {
  const {
    data: flows,
    error,
    isLoading,
  }: { data: ListMirrorsResponse; error: any; isLoading: boolean } = useSWR(
    '/api/v1/mirrors/list',
    fetcher
  );

  const cdcFlows = flows?.mirrors?.filter((flow) => flow.isCdc);
  const qrepFlows = flows?.mirrors?.filter((flow) => !flow.isCdc);

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <NewButton targetPage='/mirrors/create' buttonText='New mirror' />
          }
        >
          Mirrors
        </Header>
        <Label>
          Mirrors are used to replicate data from one peer to another. PeerDB
          supports three modes of replication.
          <br></br>
          Begin moving data in minutes by following the simple
          <Label
            as={Link}
            target='_blank'
            style={{
              color: 'teal',
              cursor: 'pointer',
              width: 'fit-content',
            }}
            href={`https://docs.peerdb.io/quickstart/quickstart`}
          >
            PeerDB Quickstart
          </Label>
        </Label>
      </Panel>
      {isLoading && (
        <Panel>
          <div className='h-screen flex items-center justify-center'>
            <ProgressCircle variant='determinate_progress_circle' />
          </div>
        </Panel>
      )}
      {!isLoading && (
        <Panel>
          <CDCFlows cdcFlows={cdcFlows} />
        </Panel>
      )}
      {!isLoading && (
        <Panel className='mt-10'>
          <QRepFlows title='Query Replication' qrepFlows={qrepFlows} />
        </Panel>
      )}
    </LayoutMain>
  );
}
