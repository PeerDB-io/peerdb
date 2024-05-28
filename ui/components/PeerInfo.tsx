'use client';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { useCallback, useEffect, useState } from 'react';
import ConfigJSONView from './ConfigJSONView';
import Link from 'next/link';

export const PeerInfo = ({ peerName }: { peerName: string }) => {
  const [info, setInfo] = useState<Peer>();

  const getPeerInfo = useCallback(async () => {
    const peerRes: Peer = await fetch(`/api/peers/info/${peerName}`, {
      cache: 'no-store',
    }).then((res) => res.json());
    setInfo(peerRes);
  }, [peerName]);

  useEffect(() => {
    getPeerInfo();
  }, [getPeerInfo]);

  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <EditPeerButton peerName={peerName} peerType={info?.type} />
      <PeerConfigDialog peerInfo={info} />
    </div>
  );
};

const EditPeerButton = ({ peerName, peerType }: { peerName: string; peerType?: DBType }) => {
  if (peerType === undefined) {
    return null;
  }

  if (!(peerType in DBType)) {
    return null;
  }

  const dbTypeName: string = DBType[peerType];
  return (
    <Link href={{ pathname: `/peers/create/${dbTypeName}`, query: { update: peerName } }}>
      <a>
        <Button variant="normal">Edit Peer</Button>
      </a>
    </Link>
  );
};

const PeerConfigDialog = ({ peerInfo }: { peerInfo?: Peer }) => (
  <Dialog
    noInteract={false}
    size='auto'
    style={{ width: '40rem' }}
    triggerButton={
      <Button variant='normalBorderless' aria-label='iconButton'>
        <Icon name='info' />
      </Button>
    }
  >
    <div style={{ display: 'flex', flexDirection: 'column', padding: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
        <Label variant='headline'>Configuration</Label>
        <DialogClose>
          <button className='IconButton' aria-label='Close'>
            <Icon name='close' />
          </button>
        </DialogClose>
      </div>
      <div>
        <div
          style={{
            height: peerInfo?.postgresConfig ? '14em' : '20em',
            whiteSpace: 'pre-wrap',
            marginTop: '1rem',
            width: '50rem',
          }}
        >
          <ConfigJSONView config={JSON.stringify(peerInfo, null, 2)} />
        </div>
      </div>
    </div>
  </Dialog>
);
