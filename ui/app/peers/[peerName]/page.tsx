import ReloadButton from '@/components/ReloadButton';
import { PeerSlotResponse, PeerStatResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import Link from 'next/link';
import SlotTable from './slottable';
import StatTable from './stattable';

type DataConfigProps = {
  params: { peerName: string };
};

const PeerData = async ({ params: { peerName } }: DataConfigProps) => {
  const getSlotData = async () => {
    const flowServiceAddr = GetFlowHttpAddressFromEnv();

    const peerSlots: PeerSlotResponse = await fetch(
      `${flowServiceAddr}/v1/peers/slots/${peerName}`
    ).then((res) => res.json());

    const slotArray = peerSlots.slotData;
    // slots with 'peerflow_slot' should come first
    slotArray?.sort((slotA, slotB) => {
      if (
        slotA.slotName.startsWith('peerflow_slot') &&
        !slotB.slotName.startsWith('peerflow_slot')
      ) {
        return -1;
      } else if (
        !slotA.slotName.startsWith('peerflow_slot') &&
        slotB.slotName.startsWith('peerflow_slot')
      ) {
        return 1;
      } else {
        return 0;
      }
    });
    return slotArray;
  };

  const getStatData = async () => {
    const flowServiceAddr = GetFlowHttpAddressFromEnv();

    const peerStats: PeerStatResponse = await fetch(
      `${flowServiceAddr}/v1/peers/stats/${peerName}`
    ).then((res) => res.json());

    return peerStats.statData;
  };

  const slots = await getSlotData();
  const stats = await getStatData();

  return (
    <div style={{ padding: '2rem', display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <div style={{ fontSize: 20, fontWeight: 'bold' }}>{peerName}</div>
        <ReloadButton />
      </div>
      {slots && stats ? (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'space-between',
            height: '70vh',
          }}
        >
          <SlotTable data={slots} />
          <StatTable data={stats} />
        </div>
      ) : (
        <div style={{ fontSize: 15, marginTop: '2rem' }}>
          We do not have stats to show for this peer at the moment. Please check
          if your PostgreSQL peer is open for connections. Note that peer
          replication slot information and stat activity is currently only
          supported for PostgreSQL peers.
          <Label
            as={Link}
            style={{
              color: 'teal',
              cursor: 'pointer',
              textDecoration: 'underline',
            }}
            target='_blank'
            href='https://docs.peerdb.io/sql/commands/supported-connectors'
          >
            More information about PeerDB connector support
          </Label>
        </div>
      )}
    </div>
  );
};

export default PeerData;
