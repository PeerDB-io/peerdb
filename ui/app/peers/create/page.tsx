'use client';
import SelectSource from '@/components/SelectSource';
import { Action } from '@/lib/Action';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithSelect } from '@/lib/Layout';
import Link from 'next/link';
import { useState } from 'react';

export default function CreatePeer() {
  const [peerType, setPeerType] = useState<string>('');
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
        <Label variant='title3'>Select source</Label>
        <Label colorName='lowContrast'>
          Start by selecting the data source for your new peer.
        </Label>
        <Action
          icon={<Icon name='help' />}
          href='https://docs.peerdb.io/sql/commands/create-peer'
          target='_blank'
          style={{ width: 'fit-content' }}
        >
          Learn about peers
        </Action>

        <RowWithSelect
          label={
            <Label as='label' htmlFor='source'>
              Data source
            </Label>
          }
          action={
            <SelectSource peerType={peerType} setPeerType={setPeerType} />
          }
        />

        <ButtonGroup style={{ marginTop: '2rem' }}>
          <Button as={Link} href='/peers'>
            Cancel
          </Button>
          <Link href={`/peers/create/${peerType}`}>
            <Button disabled={!peerType} variant='normalSolid'>
              Continue
            </Button>
          </Link>
        </ButtonGroup>
      </div>
    </div>
  );
}
