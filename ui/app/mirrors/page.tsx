'use client';

import NewButton from '@/components/NewButton';
import { QRepConfig } from '@/grpc_generated/flow';
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
  }: { data: [any]; error: any; isLoading: boolean } = useSWR(
    '/api/mirrors',
    fetcher
  );

  let cdcFlows = flows?.filter((flow) => {
    return !flow.query_string;
  });

  let qrepFlows = flows?.filter((flow) => {
    if (flow.config_proto && flow.query_string) {
      let config = QRepConfig.decode(flow.config_proto.data);
      const watermarkCol = config.watermarkColumn.toLowerCase();
      return watermarkCol !== 'xmin' && watermarkCol !== 'ctid';
    }
    return false;
  });

  let xminFlows = flows?.filter((flow) => {
    if (flow.config_proto && flow.query_string) {
      let config = QRepConfig.decode(flow.config_proto.data);
      return config.watermarkColumn.toLowerCase() === 'xmin';
    }
    return false;
  });

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
      {!isLoading && (
        <Panel className='mt-10'>
          <QRepFlows title='XMIN Mirrors' qrepFlows={xminFlows} />
        </Panel>
      )}
    </LayoutMain>
  );
}
