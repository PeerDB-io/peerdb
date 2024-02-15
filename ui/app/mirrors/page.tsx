'use client';

import { QRepConfig } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
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
            <Button as={Link} href='/mirrors/create' variant='normalSolid'>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  whiteSpace: 'nowrap',
                }}
              >
                <Icon name='add' /> <Label>New mirror</Label>
              </div>
            </Button>
          }
        >
          Mirrors
        </Header>
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
