'use client';
import { CDCConfig } from '@/app/dto/MirrorsDTO';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Label } from '@/lib/Label';
import { Divider } from '@tremor/react';
import { useState } from 'react';
import { BarLoader, ClipLoader, DotLoader } from 'react-spinners';
import { handleDropMirror } from './DropDialog';

export const ResyncDialog = ({
  mirrorConfig,
  workflowId,
}: {
  mirrorConfig:CDCConfig,
    workflowId:string
}) => {
  const [syncing, setSyncing] = useState(false);
  const [dropping, setDropping] = useState(false);
  const [msg, setMsg] = useState('');

  const handleResync = async () => {
    setMsg('Dropping existing mirror')
    const dropStatus = await handleDropMirror({
        workflowId: workflowId,
        flowJobName: mirrorConfig.flowJobName,
        sourcePeer: mirrorConfig.source!,
        destinationPeer: mirrorConfig.destination!,
        forResync: true,
    }, setDropping, setMsg);

    if(!dropStatus){
        return;
    }

    setSyncing(true)
    setMsg('Resyncing...');
    const createStatus = await fetch('/api/mirrors/cdc', {
        method: 'POST',
        body: JSON.stringify({
          mirrorConfig,
        }),
      }).then((res) => res.json());
    setSyncing(false);
    if(!createStatus.created){
        setMsg('Resyncing of existing mirror failed');
        return;
    };

    setMsg('Mirror resynced successfully');
}

  return (
    <Dialog
      noInteract={true}
      size='xLarge'
      triggerButton={
        <Button variant='normalSolid' style={{height:'2em', width:'8em'}}>
          Resync
        </Button>
      }
    >
      <div>
        <Label as='label' variant='action'>
          Resync {mirrorConfig.flowJobName}
        </Label>
        <Divider style={{ margin: 0 }} />
        <Label as='label' variant='body' style={{ marginTop: '0.3rem' }}>
          Are you sure you want to resync this mirror?
          <br></br>
          This involves <b>dropping the existing mirror</b> and recreating it.
        </Label>
    <div style={{display:'flex', alignItems:'center'}}>
        {syncing || dropping && <DotLoader size={15}/>}
        <Label as='label' style={{ marginTop: '0.3rem' }}>
            {msg}
        </Label>
    </div>
        <div style={{ display: 'flex', marginTop: '1rem' }}>
          <DialogClose>
            <Button style={{ backgroundColor: '#6c757d', color: 'white' }}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            onClick={handleResync}
            variant='normalSolid'
            style={{
              marginLeft: '1rem',
            }}
          >
            {syncing || dropping ? <BarLoader /> : 'Resync'}
          </Button>
        </div>
      </div>
    </Dialog>
  );
};
