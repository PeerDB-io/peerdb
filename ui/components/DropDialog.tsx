'use client';
import { UDropMirrorResponse } from '@/app/dto/MirrorsDTO';
import { UDropPeerResponse } from '@/app/dto/PeersDTO';
import { DeleteScript } from '@/app/scripts/handlers';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Divider } from '@tremor/react';
import { Dispatch, SetStateAction, useState } from 'react';
import { BarLoader } from 'react-spinners';

interface dropMirrorArgs {
  workflowId: string | null;
  flowJobName: string;
  sourcePeer: Peer;
  destinationPeer: Peer;
  forResync?: boolean;
}

interface dropPeerArgs {
  peerName: string;
}

interface deleteAlertArgs {
  id: number | bigint;
}

interface deleteScriptArgs {
  scriptId: number;
}

export const handleDropMirror = async (
  dropArgs: dropMirrorArgs,
  setLoading: Dispatch<SetStateAction<boolean>>,
  setMsg: Dispatch<SetStateAction<string>>
) => {
  if (!dropArgs.workflowId) {
    setMsg('Workflow ID not found for this mirror.');
    return false;
  }
  setLoading(true);
  const dropRes: UDropMirrorResponse = await fetch('/api/mirrors/drop', {
    method: 'POST',
    body: JSON.stringify(dropArgs),
  }).then((res) => res.json());
  setLoading(false);
  if (dropRes.dropped !== true) {
    setMsg(
      `Unable to drop mirror ${dropArgs.flowJobName}. ${
        dropRes.errorMessage ?? ''
      }`
    );
    return false;
  }

  setMsg('Mirror dropped successfully.');
  if (!dropArgs.forResync) {
    window.location.reload();
  }

  return true;
};

export const DropDialog = ({
  mode,
  dropArgs,
}: {
  mode: 'PEER' | 'MIRROR' | 'ALERT' | 'SCRIPT';
  dropArgs: dropMirrorArgs | dropPeerArgs | deleteAlertArgs | deleteScriptArgs;
}) => {
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');

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

  const handleDeleteAlert = async (dropArgs: deleteAlertArgs) => {
    setLoading(true);
    const deleteRes = await fetch('api/alert-config', {
      method: 'DELETE',
      body: JSON.stringify(dropArgs),
    });
    const deleteStatus = await deleteRes.text();
    setLoading(false);
    if (deleteStatus !== 'success')
      setMsg(`Unable to delete alert configuration.`);
    else {
      setMsg(`Alert configuration deleted successfully.`);
      window.location.reload();
    }
  };

  const handleDeleteScript = (dropArgs: deleteScriptArgs) => {
    setLoading(true);
    DeleteScript(dropArgs.scriptId).then((success) => {
      setLoading(false);
      if (success) window.location.reload();
    });
  };

  const getDeleteText = () => {
    let deletePart = 'Are you sure you want to delete ';
    let objectSpecificDeleteText = '';
    switch (mode) {
      case 'MIRROR':
        objectSpecificDeleteText = `mirror ${(dropArgs as dropMirrorArgs).flowJobName}`;
        break;
      case 'PEER':
        objectSpecificDeleteText = `peer ${(dropArgs as dropPeerArgs).peerName}`;
        break;
      case 'ALERT':
        objectSpecificDeleteText = 'this alert';
        break;
      case 'SCRIPT':
        objectSpecificDeleteText = 'this script';
        break;
    }
    return (
      deletePart + objectSpecificDeleteText + '? This action cannot be reverted'
    );
  };

  const handleDelete = () => {
    switch (mode) {
      case 'MIRROR':
        handleDropMirror(dropArgs as dropMirrorArgs, setLoading, setMsg);
        break;
      case 'PEER':
        handleDropPeer(dropArgs as dropPeerArgs);
        break;
      case 'ALERT':
        handleDeleteAlert(dropArgs as deleteAlertArgs);
        break;
      case 'SCRIPT':
        handleDeleteScript(dropArgs as deleteScriptArgs);
        break;
    }
  };

  return (
    <Dialog
      noInteract={true}
      size='large'
      triggerButton={
        <Button variant='drop'>
          {mode === 'ALERT' ? (
            <Label as='label' style={{ color: 'coral' }}>
              Delete
            </Label>
          ) : (
            <Icon name='delete' />
          )}
        </Button>
      }
    >
      <div>
        <Label as='label' variant='action'>
          Delete {mode.toLowerCase()}
        </Label>
        <Divider style={{ margin: 0 }} />
        <Label as='label' variant='body' style={{ marginTop: '0.3rem' }}>
          {getDeleteText()}
        </Label>
        <div style={{ display: 'flex', marginTop: '1rem' }}>
          <DialogClose>
            <Button style={{ backgroundColor: '#6c757d', color: 'white' }}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            onClick={() => handleDelete()}
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
