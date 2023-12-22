'use client';
import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import { UDropPeerResponse } from '@/app/dto/PeersDTO';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Divider } from '@tremor/react';
import { useState } from 'react';
import { BarLoader } from 'react-spinners';

interface dropMirrorArgs {
  workflowId: string | null;
  flowJobName: string;
  sourcePeer: Peer;
  destinationPeer: Peer;
}

interface dropPeerArgs {
  peerName: string;
}

export const DropDialog = ({
  mode,
  dropArgs,
}: {
  mode: 'PEER' | 'MIRROR';
  dropArgs: dropMirrorArgs | dropPeerArgs;
}) => {
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  const handleDropMirror = async (dropArgs: dropMirrorArgs) => {
    if (!dropArgs.workflowId) {
      setMsg('Workflow ID not found for this mirror.');
      return;
    }
    setLoading(true);
    const dropRes: UDropMirrorResponse = await fetch('api/mirrors/drop', {
      method: 'POST',
      body: JSON.stringify(dropArgs),
    }).then((res) => res.json());
    setLoading(false);
    if (dropRes.dropped !== true)
      setMsg(
        `Unable to drop mirror ${dropArgs.flowJobName}. ${
          dropRes.errorMessage ?? ''
        }`
      );
    else {
      setMsg('Mirror dropped successfully.');
      window.location.reload();
    }
  };

  const handleDropPeer = async (dropArgs: dropPeerArgs) => {
    if (!dropArgs.peerName) {
      setMsg('Invalid peer name');
      return;
    }

    setLoading(true);
    const dropRes: UDropPeerResponse = await fetch('api/peers/drop', {
      method: 'POST',
      body: JSON.stringify(dropArgs),
    }).then((res) => res.json());
    setLoading(false);
    if (dropRes.dropped !== true)
      setMsg(
        `Unable to drop peer ${dropArgs.peerName}. ${
          dropRes.errorMessage ?? ''
        }`
      );
    else {
      setMsg('Peer dropped successfully.');
      window.location.reload();
    }
  };

  return (
    <Dialog
      noInteract={true}
      size='large'
      triggerButton={
        <Button variant='drop' style={{ color: 'black' }}>
          <Icon name='delete' />
        </Button>
      }
    >
      <div>
        <Label as='label' variant='action'>
          Drop {mode === 'MIRROR' ? 'Mirror' : 'Peer'}
        </Label>
        <Divider style={{ margin: 0 }} />
        <Label as='label' variant='body' style={{ marginTop: '0.3rem' }}>
          Are you sure you want to drop {mode === 'MIRROR' ? 'mirror' : 'peer'}{' '}
          <b>
            {mode === 'MIRROR'
              ? (dropArgs as dropMirrorArgs).flowJobName
              : (dropArgs as dropPeerArgs).peerName}
          </b>{' '}
          ? This action cannot be reverted.
        </Label>
        <div style={{ display: 'flex', marginTop: '1rem' }}>
          <DialogClose>
            <Button style={{ backgroundColor: '#6c757d', color: 'white' }}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            onClick={() =>
              mode === 'MIRROR'
                ? handleDropMirror(dropArgs as dropMirrorArgs)
                : handleDropPeer(dropArgs as dropPeerArgs)
            }
            style={{
              marginLeft: '1rem',
              backgroundColor: '#dc3545',
              color: 'white',
            }}
          >
            {loading ? <BarLoader /> : 'Drop'}
          </Button>
        </div>
        {msg && (
          <Label
            as='label'
            style={{ color: msg.includes('success') ? 'green' : '#dc3545' }}
          >
            {msg}
          </Label>
        )}
      </div>
    </Dialog>
  );
};
