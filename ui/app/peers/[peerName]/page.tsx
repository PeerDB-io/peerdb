import ReloadButton from '@/components/ReloadButton';
import { PeerSlotResponse, PeerStatResponse } from '@/grpc_generated/route';
import { GetFlowHttpAddressFromEnv } from '@/rpc/http';
import { SlotTable, StatTable } from './datatables';
export const dynamic = 'force-dynamic';

type DataConfigProps = {
  params: { peerName: string };
};

const PeerData = async ({ params: { peerName } }: DataConfigProps) => {
  const getSlotData = async () => {
    const flowServiceAddr = GetFlowHttpAddressFromEnv();

    const peerSlots: PeerSlotResponse = await fetch(
      `${flowServiceAddr}/v1/peers/slots/${peerName}`
    ).then((res) => res.json());

    return peerSlots.slotData;
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
        <div style={{ fontSize: 25, fontWeight: 'bold' }}>{peerName}</div>
        <ReloadButton />
      </div>
      {slots && stats ? (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            height: '80vh',
          }}
        >
          <SlotTable data={slots} />
          <StatTable data={stats} />
        </div>
      ) : (
        <div style={{ fontSize: 15 }}>
          We do not have stats to show for this peer at the moment. Please note
          that peer replication slot information and stat activity is currently
          only supported for PostgreSQL peers.
        </div>
      )}
    </div>
  );
};

export default PeerData;
