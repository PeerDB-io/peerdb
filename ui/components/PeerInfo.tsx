'use client';
import { ConfigJSONView } from '@/app/alert-config/page';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { useCallback, useEffect, useState } from 'react';

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
    <Dialog
      noInteract={false}
      size='auto'
      style={{ width: '40rem' }}
      triggerButton={
        <Button style={{ backgroundColor: 'white', border: '1px solid gray' }}>
          <Label as='label' style={{ fontSize: 15 }}>
            Details
          </Label>
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
              height: info?.postgresConfig ? '14em' : '20em',
              whiteSpace: 'pre-wrap',
              marginTop: '1rem',
              width: '50rem',
            }}
          >
            <ConfigJSONView config={JSON.stringify(info, null, 2)} />
          </div>
        </div>
      </div>
    </Dialog>
  );
};
