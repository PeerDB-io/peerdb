'use client';
import SelectSource from '@/components/SelectSource';
import { Action } from '@/lib/Action';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';

export default function CreatePeer() {
  return (
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        rowGap: '1rem',
      }}
    >
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          rowGap: '1rem',
        }}
      >
        <Label variant='title3'>Select data store</Label>
        <Label colorName='lowContrast'>
          Start by selecting the data store for your new peer.
        </Label>
        <Action
          icon={<Icon name='help' />}
          href='https://docs.peerdb.io/sql/commands/create-peer'
          target='_blank'
          style={{ width: 'fit-content' }}
        >
          Learn about peers
        </Action>
        <SelectSource />
      </div>
    </div>
  );
}
