'use client';
import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Divider } from '@tremor/react';
import { useState } from 'react';
import { BarLoader } from 'react-spinners';

export const DropDialog = (dropArgs: {
  workflowId: string | null;
  flowJobName: string;
  sourcePeer: Peer;
  destinationPeer: Peer;
}) => {
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  const handleDrop = async () => {
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
      setMsg('Something went wrong when trying to drop the mirror.');
  };

  return (
    <Dialog
      modal={true}
      size='large'
      triggerButton={
        <Button style={{ color: 'white', backgroundColor: '#dc3545' }}>
          <Icon name='delete' />
        </Button>
      }
    >
      <div>
        <Label as='label' variant='action'>
          Drop Mirror
        </Label>
        <Divider style={{ margin: 0 }} />
        <Label as='label' variant='body' style={{ marginTop: '0.3rem' }}>
          Are you sure you want to drop mirror <b>{dropArgs.flowJobName}</b> ?
          This action cannot be reverted.
        </Label>
        <div style={{ display: 'flex', marginTop: '1rem' }}>
          <DialogClose>
            <Button style={{ backgroundColor: '#6c757d', color: 'white' }}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            onClick={handleDrop}
            style={{
              marginLeft: '1rem',
              backgroundColor: '#dc3545',
              color: 'white',
            }}
          >
            {loading ? <BarLoader /> : 'Delete'}
          </Button>
        </div>
        {msg && (
          <Label as='label' style={{ color: '#dc3545' }}>
            {msg}
          </Label>
        )}
      </div>
    </Dialog>
  );
};
