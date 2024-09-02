'use client';
import { changeFlowState } from '@/app/mirrors/[mirrorId]/handlers';
import { DeleteScript } from '@/app/scripts/handlers';
import { FlowStatus } from '@/grpc_generated/flow';
import { DropPeerResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Dialog, DialogClose } from '@/lib/Dialog';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithCheckbox } from '@/lib/Layout';
import { Divider } from '@tremor/react';
import { Dispatch, SetStateAction, useState } from 'react';
import { BarLoader } from 'react-spinners';

interface dropMirrorArgs {
  flowJobName: string;
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
  setMsg: Dispatch<SetStateAction<string>>,
  dropStats: boolean
) => {
  setLoading(true);
  const res = await changeFlowState(
    dropArgs.flowJobName,
    FlowStatus.STATUS_TERMINATED,
    dropStats
  );
  setLoading(false);
  if (res.status !== 200) {
    setMsg(`Unable to drop mirror ${dropArgs.flowJobName}.`);
    return false;
  }

  setMsg('Mirror dropped successfully.');
  window.location.reload();

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
  const [dropStats, setDropStats] = useState(true);

  const handleDropPeer = async (dropArgs: dropPeerArgs) => {
    if (!dropArgs.peerName) {
      setMsg('Invalid peer name');
      return;
    }

    setLoading(true);
    const dropRes: DropPeerResponse = await fetch('api/v1/peers/drop', {
      method: 'POST',
      body: JSON.stringify(dropArgs),
    }).then((res) => res.json());
    setLoading(false);
    if (dropRes.ok !== true)
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
    const deleteRes = await fetch(`api/v1/alerts/config/${dropArgs.id}`, {
      method: 'DELETE',
    });
    setLoading(false);
    if (!deleteRes.ok) setMsg(`Unable to delete alert configuration.`);
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
    return `Are you sure you want to delete ${objectSpecificDeleteText}? This action cannot be reverted`;
  };

  const handleDelete = () => {
    switch (mode) {
      case 'MIRROR':
        return handleDropMirror(
          dropArgs as dropMirrorArgs,
          setLoading,
          setMsg,
          dropStats
        );
      case 'PEER':
        return handleDropPeer(dropArgs as dropPeerArgs);
      case 'ALERT':
        return handleDeleteAlert(dropArgs as deleteAlertArgs);
      case 'SCRIPT':
        return handleDeleteScript(dropArgs as deleteScriptArgs);
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
        <RowWithCheckbox
          label={<Label>Delete mirror stats</Label>}
          action={
            <Checkbox
              checked={dropStats}
              onCheckedChange={(state: boolean) => setDropStats(state)}
            />
          }
        />
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
