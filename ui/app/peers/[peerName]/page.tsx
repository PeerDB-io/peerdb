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
    ).then((res) => {
      return res.json();
    });
    return peerSlots.slotData;
  };
  const getStatData = async () => {
    const flowServiceAddr = GetFlowHttpAddressFromEnv();
    const peerStats: PeerStatResponse = await fetch(
      `${flowServiceAddr}/v1/peers/stats/${peerName}`
    ).then((res) => {
      return res.json();
    });
    return peerStats.statData;
  };
  const slots = await getSlotData();
  const stats = await getStatData();
  return (
    <div style={{ padding: '2rem', display: 'flex', flexDirection: 'column' }}>
      <div style={{ fontSize: 25, fontWeight: 'bold' }}>{peerName}</div>
      {slots && stats ? (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            height: '80vh',
            justifyContent: 'space-around',
          }}
        >
          <SlotTable data={slots} />
          <StatTable data={stats} />
        </div>
      ) : (
        <div style={{ fontSize: 15 }}>
          Peer replication slot information and stat activity is currently only
          supported for PostgreSQL peers. Specific peer information for other
          peers coming soon!
        </div>
      )}
    </div>
  );
};

export default PeerData;
