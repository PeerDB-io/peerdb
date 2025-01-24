import { PeerInfo } from '@/components/PeerInfo';
import ReloadButton from '@/components/ReloadButton';
import React from 'react';
import LagGraph from './lagGraph';
import SlotTable from './slottable';
import StatTable from './stattable';

type DataConfigProps = {
  params: Promise<{ peerName: string }>;
};

export default function PeerData({ params }: DataConfigProps) {
  const { peerName } = React.use(params);
  return (
    <div
      style={{
        padding: '2rem',
        width: '100%',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <div
          style={{
            fontSize: 20,
            fontWeight: 'bold',
            display: 'flex',
            alignItems: 'center',
            columnGap: '1rem',
          }}
        >
          <div>{peerName}</div>
          <PeerInfo peerName={peerName} />
        </div>
        <ReloadButton />
      </div>

      <div>
        <SlotTable peerName={peerName} />
        <LagGraph peerName={peerName} />
        <StatTable peerName={peerName} />
      </div>
    </div>
  );
}
