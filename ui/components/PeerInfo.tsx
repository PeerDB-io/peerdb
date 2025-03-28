'use client';
import { PeerInfoResponse, PeerTypeResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import Link from 'next/link';
import { useCallback, useEffect, useState } from 'react';
import ConfigJSONView from './ConfigJSONView';

export function PeerInfo({ peerName }: { peerName: string }) {
  const [info, setInfo] = useState<PeerInfoResponse>();

  const getPeerInfo = useCallback(async () => {
    const peerRes: PeerInfoResponse = await fetch(
      `/api/v1/peers/info/${peerName}`,
      {
        cache: 'no-store',
      }
    ).then((res) => res.json());
    setInfo(peerRes);
  }, [peerName]);

  useEffect(() => {
    getPeerInfo();
  }, [getPeerInfo]);

  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <EditPeerButton peerName={peerName} />
      <PeerConfigDialog peerInfo={info} />
    </div>
  );
}

async function getPeerType(peerName: string): Promise<string> {
  const peerTypeRes: PeerTypeResponse = await fetch(
    `/api/v1/peers/type/${peerName}`,
    {
      cache: 'no-store',
    }
  ).then((res) => res.json());
  return peerTypeRes.peerType;
}

function EditPeerButton({ peerName }: { peerName: string }) {
  const [peerType, setPeerType] = useState<string>();
  useEffect(() => {
    getPeerType(peerName).then((type) => setPeerType(type));
  }, [peerName]);

  return (
    <Button
      as={Link}
      href={{
        pathname: `/peers/create/${peerType}`,
        query: { update: peerName },
      }}
    >
      Edit Peer
    </Button>
  );
}

function PeerConfigDialog({ peerInfo }: { peerInfo?: PeerInfoResponse }) {
  return (
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
      <div
        style={{ display: 'flex', flexDirection: 'column', padding: '1rem' }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginBottom: '0.5rem',
          }}
        >
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
              height: peerInfo?.peer?.postgresConfig ? '14em' : '20em',
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
}
