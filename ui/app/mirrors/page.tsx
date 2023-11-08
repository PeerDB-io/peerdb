import { QRepConfig } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import Link from 'next/link';
import { getTruePeer } from '../api/peers/route';
import prisma from '../utils/prisma';
import { CDCFlows, QRepFlows } from './tables';
export const dynamic = 'force-dynamic';

const stringifyConfig = (flowArray: any[]) => {
  flowArray.forEach((flow) => {
    if (flow.config_proto) {
      flow.config_proto = new TextDecoder().decode(flow.config_proto);
    }
  });
};

export default async function Mirrors() {
  let mirrors = await prisma.flows.findMany({
    include: {
      sourcePeer: true,
      destinationPeer: true,
    },
  });

  const flows = mirrors.map((mirror) => {
    let newMirror: any = {
      ...mirror,
      sourcePeer: getTruePeer(mirror.sourcePeer),
      destinationPeer: getTruePeer(mirror.destinationPeer),
    };
    return newMirror;
  });

  let cdcFlows = flows.filter((flow) => {
    return !flow.query_string;
  });

  let qrepFlows = flows.filter((flow) => {
    if (flow.config_proto && flow.query_string) {
      let config = QRepConfig.decode(flow.config_proto);
      const watermarkCol = config.watermarkColumn.toLowerCase();
      return watermarkCol !== 'xmin' && watermarkCol !== 'ctid';
    }
    return false;
  });

  let xminFlows = flows.filter((flow) => {
    if (flow.config_proto && flow.query_string) {
      let config = QRepConfig.decode(flow.config_proto);
      return config.watermarkColumn.toLowerCase() === 'xmin';
    }
    return false;
  });

  stringifyConfig(cdcFlows);
  stringifyConfig(qrepFlows);
  stringifyConfig(xminFlows);

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button
              as={Link}
              style={{
                width: '10%',
                height: '2rem',
                fontSize: 17,
                boxShadow: '0px 2px 4px rgba(0,0,0,0.2)',
              }}
              href={'/mirrors/create'}
              variant='normalSolid'
            >
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
      <Panel>
        <CDCFlows cdcFlows={cdcFlows} />
      </Panel>
      <Panel className='mt-10'>
        <QRepFlows title='Query Replication' qrepFlows={qrepFlows} />
      </Panel>
      <Panel className='mt-10'>
        <QRepFlows title='XMIN Mirrors' qrepFlows={xminFlows} />
      </Panel>
    </LayoutMain>
  );
}
